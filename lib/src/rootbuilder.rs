//! APIs to construct a root filesystem
//!

use std::{collections::HashSet, process::Command};

use anyhow::{Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use cap_std::fs::Dir;
use cap_std_ext::cap_std;
use cap_std_ext::prelude::CapStdExtCommandExt;
use fn_error_context::context;

use crate::utils::ensure_relative_path;

const FALLBACK_PATH: &str = "usr/sbin:usr/bin";

#[context("Gathering dependencies via ldd of {p}")]
fn dependencies(src_root: &Dir, p: &Utf8Path, deps: &mut HashSet<Utf8PathBuf>) -> Result<()> {
    // Helper closure to recursively resolve dependencies if target is not already in the set
    let recurse = |target: &Utf8Path, deps: &mut HashSet<Utf8PathBuf>| {
        assert!(target.is_relative());
        if !deps.contains(target) {
            dependencies(src_root, target, deps)?;
            deps.insert(target.into());
        }
        anyhow::Ok(())
    };
    // The vDSO is a special case that we should ignore
    const LINUX_VDSO: &'static str = "linux-vdso.so";
    // We parse the output of ldd, like everyone else (e.g. dracut).
    let o = Command::new("ldd")
        .arg(p)
        .cwd_dir(src_root.try_clone()?)
        .output()?;
    let st = o.status;
    if !st.success() {
        anyhow::bail!("Failed to run ldd: {st:?}");
    }
    let stdout = String::from_utf8(o.stdout).context("Failed to parse ldd output")?;
    for line in stdout.lines() {
        let line = line.trim();
        let mut parts = line.split_ascii_whitespace();
        let first = if let Some(l) = parts.next() {
            l
        } else {
            continue;
        };
        // Ignore the vDSO
        if first.starts_with(LINUX_VDSO) {
            continue;
        } else if first.contains("/ld-linux") {
            // If it's the dynamic loader, capture that.
            recurse(ensure_relative_path(first.into()), deps)?;
        }
        let token = if let Some(l) = parts.next() {
            l
        } else {
            continue;
        };
        // Normal lines look like:
        //   libtinfo.so.6 => /lib64/libtinfo.so.6 (0x00007f6da59a6000)
        if token == "=>" {
            let libpath = if let Some(l) = parts.next() {
                l
            } else {
                anyhow::bail!("Invalid output from ldd: ")
            };
            recurse(ensure_relative_path(libpath.into()), deps)?;
        }
    }
    Ok(())
}

#[context("Populating root with dependencies")]
pub(crate) fn populate_root_with_dependencies(
    src_root: &Dir,
    sources: &[&Utf8Path],
    dest_root: &Dir,
) -> Result<()> {
    let mut deps = HashSet::new();
    for src in sources {
        let src = ensure_relative_path(src);
        dependencies(src_root, src, &mut deps)?;
        deps.insert(src.to_owned());
    }
    tracing::debug!(
        "Found {} dependencies of {} sources",
        deps.len(),
        sources.len()
    );

    for dep in deps {
        let dep = dep.as_path();
        if let Some(parent) = dep.parent() {
            dest_root
                .create_dir_all(parent)
                .with_context(|| format!("Creating {parent}"))?;
        }
        let mut destf = dest_root
            .create(dep)
            .with_context(|| format!("Opening {dep} for write"))?;
        let mut srcf = src_root
            .open(dep)
            .with_context(|| format!("Opening {dep}"))?;
        let srcmeta = srcf.metadata()?;
        std::io::copy(&mut srcf, &mut destf).with_context(|| format!("Copying {dep}"))?;
        destf
            .set_permissions(srcmeta.permissions())
            .context("Setting permissions")?;
    }

    Ok(())
}

fn find_exe(root: &Dir, name: &str) -> Result<Option<Utf8PathBuf>> {
    let path = std::env::var_os("PATH");
    let search_paths = path
        .as_ref()
        .and_then(|p| p.to_str())
        .unwrap_or(FALLBACK_PATH)
        .split(':');
    for path in search_paths {
        let path = ensure_relative_path(path.into()).join(name);
        if root.exists(&path) {
            return Ok(Some(path));
        }
    }
    Ok(None)
}

#[context("Populating rootfs")]
pub(crate) fn copy_self_to_root(target: Utf8PathBuf) -> Result<()> {
    let src = Dir::open_ambient_dir("/", cap_std::ambient_authority())?;
    let self_path: Utf8PathBuf = std::fs::read_link("/proc/self/exe")
        .context("Reading /proc/self/exe")?
        .try_into()?;
    let dest = Dir::open_ambient_dir(target, cap_std::ambient_authority())?;
    let sources = [self_path.as_path()].into_iter();
    #[cfg(feature = "install")]
    let sources = sources.chain(
        crate::install::BIN_DEPENDENCIES
            .iter()
            .map(|&v| Utf8Path::new(v)),
    );
    let sources = sources.collect::<Vec<_>>();
    crate::rootbuilder::populate_root_with_dependencies(&src, sources.as_slice(), &dest)?;
    Ok(())
}
