//! Consistenty checking

use std::os::fd::AsFd;

use anyhow::Ok;
use anyhow::{Context, Result};
use ostree_ext::ostree;
use ostree_ext::ostree_prepareroot::Tristate;
use ostree_ext::repoext::RepoExt;
use serde::{Deserialize, Serialize};

use crate::imgstorage::repo_get_fsverity;
use crate::store::Storage;

/// Output from a fsck operation.
///
/// Stability: none (may change arbitrarily)
#[derive(Default, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct FsckResult {
    pub(crate) errors: Vec<String>,
    pub verity_enabled: u64,
}

async fn validate_verity(repo: &ostree::Repo) -> Result<(u64, Result<()>)> {
    let verity_state = repo_get_fsverity(repo)?;

    let repo = repo.clone();
    let (enabled, disabled) = tokio::task::spawn_blocking(move || {
        let (mut enabled, mut disabled) = (0u64, 0u64);
        repo.traverse_regfile_objects::<(), _>(|name, f| {
            let r: Option<crate::fsverity::Sha256HashValue> =
                crate::fsverity::ioctl::fs_ioc_measure_verity_optional(f.as_fd())
                    .with_context(|| format!("Querying verity for {name}"))?;
            if r.is_some() {
                enabled += 1;
            } else {
                disabled += 1;
            }
            Ok(std::ops::ControlFlow::Continue(()))
        })?;
        Ok((enabled, disabled))
    })
    .await??;
    if verity_state == Tristate::Enabled && disabled > 0 {
        return Ok((
            enabled,
            Err(anyhow::anyhow!(
                "Found objects missing verity: {}",
                disabled
            )),
        ));
    }
    Ok((enabled, Ok(())))
}

pub(crate) async fn fsck(storage: &Storage) -> Result<FsckResult> {
    let mut r = FsckResult::default();
    let (verity_enabled, verity_err) = crate::utils::async_task_with_spinner(
        "Checking fsverity",
        validate_verity(&storage.repo()),
    )
    .await?;
    r.verity_enabled = verity_enabled;
    if let Err(e) = verity_err {
        r.errors.push(e.to_string());
    }
    serde_json::to_writer_pretty(std::io::stdout().lock(), &r)?;
    Ok(r)
}
