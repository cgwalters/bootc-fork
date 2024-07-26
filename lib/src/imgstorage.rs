//! # bootc-managed container storage
//!
//! The default storage for this project uses ostree, canonically storing all of its state in
//! `/sysroot/ostree`.
//!
//! This containers-storage: which canonically lives in `/sysroot/ostree/bootc`.

use std::io::{Read, Seek};
use std::process::Command;
use std::sync::Arc;

use anyhow::{Context, Ok, Result};
use camino::Utf8Path;
use cap_std_ext::cap_std::fs::Dir;
use cap_std_ext::cmdext::CapStdExtCommandExt;
use cap_std_ext::dirext::CapStdExtDirExt;
use fn_error_context::context;
use std::os::fd::OwnedFd;

use crate::task::Task;
use crate::utils::{AsyncCommandRunExt, CommandRunExt};

/// The path to the storage, relative to the physical system root.
pub(crate) const SUBPATH: &str = "ostree/bootc/storage";
/// The path to the "runroot" with transient runtime state; this is
/// relative to the /run directory
const RUNROOT: &str = "bootc/storage";
pub(crate) struct Storage {
    /// The root directory
    sysroot: Dir,
    /// The location of container storage
    storage_root: Dir,
    #[allow(dead_code)]
    /// Our runtime state
    run: Dir,
}

impl Storage {
    fn podman_cmd_in(sysroot: OwnedFd, run: OwnedFd) -> Result<Command> {
        let mut t = Command::new("podman");
        // podman expects absolute paths for these, so use /proc/self/fd
        {
            let sysroot_fd: Arc<OwnedFd> = Arc::new(sysroot);
            t.take_fd_n(sysroot_fd, 3);
        }
        {
            let run_fd: Arc<OwnedFd> = Arc::new(run);
            t.take_fd_n(run_fd, 4);
        }
        t.args(["--root=/proc/self/fd/3", "--runroot=/proc/self/fd/4"]);
        Ok(t)
    }

    /// Create a `podman image` Command instance prepared to operate on our alternative
    /// root.
    pub(crate) fn new_image_cmd(&self) -> Result<Command> {
        let sysroot = self.storage_root.try_clone()?.into_std_file().into();
        let run = self.run.try_clone()?.into_std_file().into();
        let mut r = Self::podman_cmd_in(sysroot, run)?;
        // We want to limit things to only manipulating images by default.
        r.arg("image");
        Ok(r)
    }

    pub(crate) fn new_async_image_cmd(&self) -> Result<tokio::process::Command> {
        let mut r = tokio::process::Command::from(self.new_image_cmd()?);
        r.kill_on_drop(true);
        Ok(r)
    }

    async fn run_podman_image_async<S>(&self, args: impl IntoIterator<Item = S>) -> Result<()>
    where
        S: AsRef<str>,
    {
        let mut cmd = self.new_async_image_cmd()?;
        let mut stderr = tempfile::tempfile()?;
        cmd.stderr(stderr.try_clone()?);
        if let Err(e) = cmd.run().await {
            stderr.seek(std::io::SeekFrom::Start(0))?;
            let mut stderr_buf = String::new();
            // Ignore errors
            let _ = stderr.read_to_string(&mut stderr_buf);
            return Err(anyhow::anyhow!("{e}: {stderr_buf}"));
        }
        Ok(())
    }

    #[context("Creating imgstorage")]
    pub(crate) fn create(sysroot: &Dir, run: &Dir) -> Result<Self> {
        let subpath = Utf8Path::new(SUBPATH);
        // SAFETY: We know there's a parent
        let parent = subpath.parent().unwrap();
        if !sysroot.try_exists(subpath)? {
            let tmp = format!("{SUBPATH}.tmp");
            sysroot.remove_all_optional(&tmp)?;
            sysroot.create_dir_all(parent)?;
            sysroot.create_dir_all(&tmp).context("Creating tmpdir")?;
            // There's no explicit API to initialize a containers-storage:
            // root, simply passing a path will attempt to auto-create it.
            // We run "podman images" in the new root.
            Self::podman_cmd_in(sysroot.open_dir(&tmp)?.into(), run.try_clone()?.into())?
                .arg("images")
                .run()?;
            sysroot
                .rename(&tmp, sysroot, subpath)
                .context("Renaming tmpdir")?;
        }
        Self::open(sysroot, run)
    }

    #[context("Opening imgstorage")]
    pub(crate) fn open(sysroot: &Dir, run: &Dir) -> Result<Self> {
        let storage_root = sysroot.open_dir(SUBPATH).context(SUBPATH)?;
        // Always auto-create this if missing
        run.create_dir_all(RUNROOT)?;
        let run = run.open_dir(RUNROOT).context(RUNROOT)?;
        Ok(Self {
            sysroot: sysroot.try_clone()?,
            storage_root,
            run,
        })
    }

    /// Fetch the image if it is not already present; return whether
    /// or not the image was fetched.
    pub(crate) async fn pull(&self, image: &str) -> Result<bool> {
        // Sadly https://docs.rs/containers-image-proxy/latest/containers_image_proxy/struct.ImageProxy.html#method.open_image_optional
        // doesn't work with containers-storage yet
        let mut cmd = self.new_async_image_cmd()?;
        cmd.args(["exists", image]);
        let exists = cmd.status().await?.success();
        if exists {
            // The image exists, false means we didn't pull it
            Ok(false)
        } else {
            let mut cmd = self.new_async_image_cmd()?;
            cmd.args(["pull", image]);
            let authfile = ostree_ext::globals::get_global_authfile(&self.sysroot)?
                .map(|(authfile, _fd)| authfile);
            if let Some(authfile) = authfile {
                cmd.args(["--authfile", authfile.as_str()]);
            }
            cmd.run().await.context("Failed to pull image")?;
            Ok(true)
        }
    }

    pub(crate) fn pull_from_host_storage(&self, image: &str) -> Result<()> {
        // The skopeo API expects absolute paths, so we make a temporary bind
        let temp_mount = crate::mount::TempMount::new(&self.storage_root)?;
        let temp_mount_path = temp_mount.path();
        // And an ephemeral place for the transient state
        let tmp_runroot = tempfile::tempdir()?;
        let tmp_runroot: &Utf8Path = tmp_runroot.path().try_into()?;

        // The destination (target stateroot) + container storage dest
        let storage_dest = &format!("containers-storage:[overlay@{temp_mount_path}+{tmp_runroot}]");
        Task::new(format!("Copying image to target: {}", image), "podman")
            .arg("push")
            .arg(image)
            .arg(format!("{storage_dest}{image}"))
            .run()?;
        temp_mount.close()?;
        Ok(())
    }
}
