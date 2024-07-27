//! # bootc-managed container storage
//!
//! The default storage for this project uses ostree, canonically storing all of its state in
//! `/sysroot/ostree`.
//!
//! This containers-storage: which canonically lives in `/sysroot/ostree/bootc`.

use std::io::{Read, Seek};
use std::os::unix::process::CommandExt;
use std::process::Command;
use std::sync::Arc;

use anyhow::{Context, Result};
use camino::Utf8Path;
use cap_std_ext::cap_std;
use cap_std_ext::cap_std::fs::Dir;
use cap_std_ext::dirext::CapStdExtDirExt;
use fn_error_context::context;
use std::os::fd::AsFd;
use tokio::process::Command as AsyncCommand;

use crate::mount::bind_mount_fd;
use crate::utils::{rustix_err_context, AsyncCommandRunExt, CommandRunExt};

/// Global directory path which we use for podman to point
/// it at our storage.
pub(crate) const STORAGE_ALIAS_DIR: &str = "/run/bootc/storage";
/// And a similar alias for the runtime state.
pub(crate) const STORAGE_RUN_ALIAS_DIR: &str = "/run/bootc/run-storage";

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

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PullMode {
    /// Pull only if the image is not present
    IfNotExists,
    /// Always check for an update
    #[allow(dead_code)]
    Always,
}

async fn run_cmd_async(cmd: Command) -> Result<()> {
    let mut cmd = tokio::process::Command::from(cmd);
    cmd.kill_on_drop(true);
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

#[allow(unsafe_code)]
#[context("Binding storage roots")]
fn bind_storage_roots(cmd: &mut Command, storage_root: &Dir, run_root: &Dir) -> Result<()> {
    // podman requires an absolute path, for two reasons right now:
    // - It writes the file paths into `db.sql`, a sqlite database for unknown reasons
    // - It forks helper binaries, so just giving it /proc/self/fd won't work as
    //   those helpers may not get the fd passed. (which is also true of skopeo)
    // We create a new mount namespace, which also has the helpful side effect
    // of automatically cleaning up the global bind mount that the storage stack
    // creates.

    let storage_root = Arc::new(storage_root.try_clone().context("Cloning storage root")?);
    let run_root = Arc::new(run_root.try_clone().context("Cloning runroot")?);
    // SAFETY: All the APIs we call here are safe to invoke between fork and exec.
    unsafe {
        cmd.pre_exec(move || {
            use rustix::thread::unshare;
            rustix_err_context(unshare(rustix::thread::UnshareFlags::NEWNS), "unshare").unwrap();
            bind_mount_fd(storage_root.as_fd(), STORAGE_ALIAS_DIR).unwrap();
            bind_mount_fd(run_root.as_fd(), STORAGE_RUN_ALIAS_DIR).unwrap();
            // And back to / just by default
            rustix_err_context(rustix::process::chdir("/"), "chdir").unwrap();
            Ok(())
        })
    };
    Ok(())
}

fn new_podman_cmd_in(storage_root: &Dir, run_root: &Dir) -> Result<Command> {
    let mut cmd = Command::new("podman");
    bind_storage_roots(&mut cmd, storage_root, run_root)?;
    cmd.args([
        "--root",
        STORAGE_ALIAS_DIR,
        "--runroot",
        STORAGE_RUN_ALIAS_DIR,
    ]);
    Ok(cmd)
}

impl Storage {
    /// Create a `podman image` Command instance prepared to operate on our alternative
    /// root.
    pub(crate) fn new_image_cmd(&self) -> Result<Command> {
        let mut r = new_podman_cmd_in(&self.storage_root, &self.run)?;
        // We want to limit things to only manipulating images by default.
        r.arg("image");
        Ok(r)
    }

    fn init_globals() -> Result<()> {
        // Ensure our global storage alias dirs exist
        for d in [STORAGE_ALIAS_DIR, STORAGE_RUN_ALIAS_DIR] {
            std::fs::create_dir_all(d).with_context(|| format!("Creating {d}"))?;
        }
        Ok(())
    }

    #[context("Creating imgstorage")]
    pub(crate) fn create(sysroot: &Dir, run: &Dir) -> Result<Self> {
        Self::init_globals()?;
        let subpath = Utf8Path::new(SUBPATH);
        // SAFETY: We know there's a parent
        let parent = subpath.parent().unwrap();
        if !sysroot
            .try_exists(subpath)
            .with_context(|| format!("Querying {subpath}"))?
        {
            let tmp = format!("{SUBPATH}.tmp");
            sysroot.remove_all_optional(&tmp).context("Removing tmp")?;
            sysroot
                .create_dir_all(parent)
                .with_context(|| format!("Creating {parent}"))?;
            sysroot.create_dir_all(&tmp).context("Creating tmpdir")?;
            let storage_root = sysroot.open_dir(&tmp).context("Open tmp")?;
            // There's no explicit API to initialize a containers-storage:
            // root, simply passing a path will attempt to auto-create it.
            // We run "podman images" in the new root.
            new_podman_cmd_in(&storage_root, &run)?
                .arg("images")
                .run()
                .context("Initializing images")?;
            drop(storage_root);
            sysroot
                .rename(&tmp, sysroot, subpath)
                .context("Renaming tmpdir")?;
        }
        Self::open(sysroot, run)
    }

    #[context("Opening imgstorage")]
    pub(crate) fn open(sysroot: &Dir, run: &Dir) -> Result<Self> {
        Self::init_globals()?;
        let storage_root = sysroot
            .open_dir(SUBPATH)
            .with_context(|| format!("Opening {SUBPATH}"))?;
        // Always auto-create this if missing
        run.create_dir_all(RUNROOT)
            .with_context(|| format!("Creating {RUNROOT}"))?;
        let run = run.open_dir(RUNROOT)?;
        Ok(Self {
            sysroot: sysroot.try_clone()?,
            storage_root,
            run,
        })
    }

    /// Fetch the image if it is not already present; return whether
    /// or not the image was fetched.
    pub(crate) async fn pull(&self, image: &str, mode: PullMode) -> Result<bool> {
        match mode {
            PullMode::IfNotExists => {
                // Sadly https://docs.rs/containers-image-proxy/latest/containers_image_proxy/struct.ImageProxy.html#method.open_image_optional
                // doesn't work with containers-storage yet
                let mut cmd = AsyncCommand::from(self.new_image_cmd()?);
                cmd.args(["exists", image]);
                let exists = cmd.status().await?.success();
                if exists {
                    return Ok(false);
                }
            }
            PullMode::Always => {}
        };
        let mut cmd = self.new_image_cmd()?;
        cmd.args(["pull", image]);
        let authfile = ostree_ext::globals::get_global_authfile(&self.sysroot)?
            .map(|(authfile, _fd)| authfile);
        if let Some(authfile) = authfile {
            cmd.args(["--authfile", authfile.as_str()]);
        }
        run_cmd_async(cmd).await.context("Failed to pull image")?;
        Ok(true)
    }

    pub(crate) async fn pull_from_host_storage(&self, image: &str) -> Result<()> {
        let mut cmd = Command::new("podman");
        // An ephemeral place for the transient state
        let temp_runroot = cap_std_ext::cap_tempfile::TempDir::new(cap_std::ambient_authority())?;
        bind_storage_roots(&mut cmd, &self.storage_root, &temp_runroot)?;

        // The destination (target stateroot) + container storage dest
        let storage_dest =
            &format!("containers-storage:[overlay@{STORAGE_ALIAS_DIR}+{STORAGE_RUN_ALIAS_DIR}]");
        cmd.arg(image).arg(format!("{storage_dest}{image}"));
        run_cmd_async(cmd).await?;
        temp_runroot.close()?;
        Ok(())
    }
}
