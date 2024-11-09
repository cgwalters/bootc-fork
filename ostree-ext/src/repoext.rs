//! Extensions for ostree repository operations.

use std::ops::ControlFlow;

use anyhow::{Context, Result};
use camino::Utf8PathBuf;
use cap_std::fs::Dir;
use fn_error_context::context;
use ocidir::cap_std;

/// Extensions for OSTree repos
pub trait RepoExt {
    /// Traverse all regular file objects.
    fn traverse_regfile_objects<B, F>(&self, f: F) -> Result<ControlFlow<B>>
    where
        F: FnMut(&str, &mut std::fs::File) -> Result<ControlFlow<B>>;
}

fn process_objdir<B, F>(d: &Dir, prefix: &str, fileext: &str, mut f: F) -> Result<ControlFlow<B>>
where
    F: FnMut(&str, &mut std::fs::File) -> Result<ControlFlow<B>>,
{
    for ent in d.entries()? {
        let ent = ent?;
        if !ent.file_type()?.is_file() {
            continue;
        }
        let name = ent.file_name();
        let name = name
            .into_string()
            .map(Utf8PathBuf::from)
            .map_err(|_| anyhow::anyhow!("Invalid UTF-8"))?;
        let (Some(stem), Some(ext)) = (name.file_stem(), name.extension()) else {
            continue;
        };
        if ext != fileext {
            continue;
        }
        let digest = format!("{prefix}{stem}");
        let mut fd = d
            .open(&name)
            .with_context(|| format!("Failed to open {name}"))?
            .into_std();
        match f(&digest, &mut fd)? {
            ControlFlow::Continue(_) => {}
            b => return Ok(b),
        }
    }
    Ok(ControlFlow::Continue(()))
}

impl RepoExt for ostree::Repo {
    #[context("Traversing repo")]
    fn traverse_regfile_objects<B, F>(&self, mut f: F) -> Result<ControlFlow<B>>
    where
        F: FnMut(&str, &mut std::fs::File) -> Result<ControlFlow<B>>,
    {
        let repodir = Dir::reopen_dir(&self.dfd_borrow())?;
        let ext = match self.mode() {
            ostree::RepoMode::Archive => "filez",
            _ => "file",
        };

        for ent in repodir.read_dir("objects")? {
            let ent = ent?;
            if !ent.file_type()?.is_dir() {
                continue;
            }
            let name = ent.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let objdir = ent.open_dir()?;
            process_objdir::<B, _>(&objdir, name, ext, &mut f)?;
        }
        Ok(ControlFlow::Continue(()))
    }
}

#[cfg(test)]
mod tests {
    use std::os::fd::AsFd;

    use crate::fixture;

    use super::*;
    use cap_std_ext::cap_tempfile;

    #[test]
    fn test_traverse_noop() -> Result<()> {
        let td = cap_tempfile::TempDir::new(cap_std::ambient_authority())?;
        let repo = ostree::Repo::create_at_dir(td.as_fd(), ".", ostree::RepoMode::Archive, None)?;
        // Test a no-op
        repo.traverse_regfile_objects::<(), _>(|_name, _f| unreachable!())
            .unwrap();
        Ok(())
    }

    #[test]
    fn test_traverse_fixture() -> Result<()> {
        let fixture = fixture::Fixture::new_v1()?;
        let mut n = 0u32;
        fixture
            .srcrepo()
            .traverse_regfile_objects::<(), _>(|name, _f| {
                assert_eq!(name.len(), 64);
                n += 1;
                Ok(ControlFlow::Continue(()))
            })
            .unwrap();
        // Just verify this works
        assert!(n > 9);
        Ok(())
    }
}
