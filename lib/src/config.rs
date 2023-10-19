use std::collections::HashMap;
use std::io::Read;

use crate::k8sapitypes::ConfigMap;
use anyhow::{anyhow, Context, Result};
use camino::Utf8Path;
use cap_std::fs::Dir;
use cap_std_ext::cap_std;
use containers_image_proxy::ImageProxy;
use fn_error_context::context;
use ostree_ext::container as ostree_container;
use ostree_ext::oci_spec;
use ostree_ext::prelude::{Cast, FileExt, InputStreamExtManual, ToVariant};
use ostree_ext::{gio, glib, ostree};
use ostree_ext::{ostree::Deployment, sysroot::SysrootLock};
use rustix::fd::AsRawFd;
use tokio::io::AsyncReadExt;

/// The media type of a configmap stored in a registry as an OCI artifact
const MEDIA_TYPE_CONFIGMAP: &str = "application/containers.configmap+json";

const CONFIGMAP_SIZE_LIMIT: u32 = 1_048_576;

/// The prefix used to store configmaps
const REF_PREFIX: &str = "bootc/config";

/// The key used to configure the file prefix; the default is `/etc`.
const CONFIGMAP_PREFIX_ANNOTATION_KEY: &str = "bootc.prefix";
/// The default prefix for configmaps and secrets.
const DEFAULT_MOUNT_PREFIX: &str = "etc";

/// The key used to store the configmap metadata
const CONFIGMAP_MANIFEST_KEY: &str = "bootc.configmap.metadata";
/// The key used to store the etag from the HTTP request
const CONFIGMAP_ETAG_KEY: &str = "bootc.configmap.etag";

/// Default to world-readable for configmaps
const DEFAULT_MODE: u32 = 0o644;

const ORIGIN_BOOTC_CONFIG_PREFIX: &str = "bootc.config.";

/// The serialized metadata about configmaps attached to a deployment
pub(crate) struct ConfigSpec {
    pub(crate) name: String,
    pub(crate) imgref: ostree_container::ImageReference,
}

pub(crate) struct ConfigMapObject {
    manifest: oci_spec::image::ImageManifest,
    config: ConfigMap,
}

impl ConfigSpec {
    const KEY_IMAGE: &str = "imageref";

    /// Return the keyfile group name
    fn group(name: &str) -> String {
        format!("{ORIGIN_BOOTC_CONFIG_PREFIX}{name}")
    }

    /// Parse a config specification from a keyfile
    #[context("Parsing config spec")]
    fn from_keyfile(kf: &glib::KeyFile, name: &str) -> Result<Self> {
        let group = Self::group(name);
        let imgref = kf.string(&group, Self::KEY_IMAGE)?;
        let imgref = imgref.as_str().try_into()?;
        Ok(Self {
            imgref,
            name: name.to_string(),
        })
    }

    /// Serialize this config spec into the target keyfile
    fn store(&self, kf: &glib::KeyFile) {
        let group = &Self::group(&self.name);
        // Ignore errors if the group didn't exist
        let _ = kf.remove_group(group);
        kf.set_string(group, Self::KEY_IMAGE, &self.imgref.to_string());
    }

    /// Remove this config from the target; returns `true` if the value was present
    fn remove(&self, kf: &glib::KeyFile) -> bool {
        let group = &Self::group(&self.name);
        kf.remove_group(group).is_ok()
    }

    pub(crate) fn ostree_ref(&self) -> Result<String> {
        name_to_ostree_ref(&self.name)
    }
}

/// Options for internal testing
#[derive(Debug, clap::Subcommand)]
pub(crate) enum ConfigOpts {
    /// Add a remote configmap
    Add {
        /// Container registry pull specification; this must refer to an OCI artifact
        imgref: String,

        /// The transport; e.g. oci, oci-archive.  Defaults to `registry`.
        #[clap(long, default_value = "registry")]
        transport: String,

        #[clap(long)]
        /// Provide an explicit name for the map
        name: Option<String>,
    },
    /// Show a configmap (in YAML format)
    Show {
        /// Name of the configmap to show
        name: String,
    },
    /// Add a remote configmap
    Remove {
        /// Name of the configmap to remove
        name: String,
    },
    /// Check for updates for an individual configmap
    Update {
        /// Name of the configmap to update
        names: Vec<String>,
    },
    /// List attached configmaps
    List,
}

/// Implementation of the `boot config` CLI.
pub(crate) async fn run(opts: ConfigOpts) -> Result<()> {
    crate::cli::prepare_for_write().await?;
    let sysroot = &crate::cli::get_locked_sysroot().await?;
    match opts {
        ConfigOpts::Add {
            imgref,
            transport,
            name,
        } => {
            let transport = ostree_container::Transport::try_from(transport.as_str())?;
            let imgref = ostree_container::ImageReference {
                transport,
                name: imgref,
            };
            add(sysroot, &imgref, name.as_deref()).await
        }
        ConfigOpts::Remove { name } => remove(sysroot, name.as_str()).await,
        ConfigOpts::Update { names } => update(sysroot, names.into_iter()).await,
        ConfigOpts::Show { name } => show(sysroot, &name).await,
        ConfigOpts::List => list(sysroot).await,
    }
}

async fn new_proxy() -> Result<ImageProxy> {
    let mut config = containers_image_proxy::ImageProxyConfig::default();
    ostree_container::merge_default_container_proxy_opts(&mut config)?;
    containers_image_proxy::ImageProxy::new_with_config(config).await
}

#[context("Converting configmap name to ostree ref")]
fn name_to_ostree_ref(name: &str) -> Result<String> {
    ostree_ext::refescape::prefix_escape_for_ref(REF_PREFIX, name)
}

/// Retrieve the "mount prefix" for the configmap
fn get_prefix(map: &ConfigMap) -> &str {
    map.metadata
        .annotations
        .as_ref()
        .and_then(|m| m.get(CONFIGMAP_PREFIX_ANNOTATION_KEY).map(|s| s.as_str()))
        .unwrap_or(DEFAULT_MOUNT_PREFIX)
}

async fn list(sysroot: &SysrootLock) -> Result<()> {
    let merge_deployment = &crate::cli::target_deployment(sysroot)?;
    let configs = configs_for_deployment(sysroot, merge_deployment)?;
    if configs.len() == 0 {
        println!("No dynamic ConfigMap objects attached");
    } else {
        for config in configs {
            println!("{} {}", config.name.as_str(), config.imgref);
        }
    }
    Ok(())
}

fn load_config(sysroot: &SysrootLock, name: &str) -> Result<ConfigMap> {
    let cancellable = gio::Cancellable::NONE;
    let configref = name_to_ostree_ref(name)?;
    let (r, rev) = sysroot.repo().read_commit(&configref, cancellable)?;
    tracing::debug!("Inspecting {rev}");
    let commitv = sysroot.repo().load_commit(&rev)?.0;
    let commitmeta = commitv.child_value(0);
    let commitmeta = &glib::VariantDict::new(Some(&commitmeta));
    let cfgdata = commitmeta
        .lookup_value(CONFIGMAP_MANIFEST_KEY, Some(glib::VariantTy::STRING))
        .ok_or_else(|| anyhow!("Missing metadata key {CONFIGMAP_MANIFEST_KEY}"))?;
    let cfgdata = cfgdata.str().unwrap();
    let mut cfg: ConfigMap = serde_json::from_str(cfgdata)?;
    let prefix = Utf8Path::new(get_prefix(&cfg).trim_start_matches('/'));
    let d = r.child(prefix);
    if let Some(v) = cfg.binary_data.as_mut() {
        for (k, v) in v.iter_mut() {
            let k = k.trim_start_matches('/');
            d.child(k)
                .read(cancellable)?
                .into_read()
                .read_to_end(&mut v.0)?;
        }
    }
    if let Some(v) = cfg.data.as_mut() {
        for (k, v) in v.iter_mut() {
            let k = k.trim_start_matches('/');
            d.child(k)
                .read(cancellable)?
                .into_read()
                .read_to_string(v)?;
        }
    }
    Ok(cfg)
}

async fn show(sysroot: &SysrootLock, name: &str) -> Result<()> {
    let config = load_config(sysroot, name)?;
    let mut stdout = std::io::stdout().lock();
    serde_yaml::to_writer(&mut stdout, &config)?;
    Ok(())
}

async fn remove(sysroot: &SysrootLock, name: &str) -> Result<()> {
    let cancellable = gio::Cancellable::NONE;
    let repo = &sysroot.repo();
    let merge_deployment = &crate::cli::target_deployment(sysroot)?;
    let stateroot = merge_deployment.osname();
    let origin = merge_deployment
        .origin()
        .ok_or_else(|| anyhow::anyhow!("Deployment is missing an origin"))?;
    let configs = configs_for_deployment(sysroot, merge_deployment)?;
    let cfgspec = configs
        .iter()
        .find(|v| v.name == name)
        .ok_or_else(|| anyhow::anyhow!("No config with name {name}"))?;
    let removed = cfgspec.remove(&origin);
    assert!(removed);

    let cfgref = cfgspec.ostree_ref()?;
    tracing::debug!("Removing ref {cfgref}");
    repo.set_ref_immediate(None, &cfgref, None, cancellable)?;

    Ok(())
}

#[context("Writing configmap")]
fn write_configmap(
    sysroot: &SysrootLock,
    sepolicy: Option<&ostree::SePolicy>,
    spec: &ConfigSpec,
    cfgobj: &ConfigMapObject,
    cancellable: Option<&gio::Cancellable>,
) -> Result<()> {
    use crate::ostree_generation::{create_and_commit_dirmeta, write_file};
    let name = spec.name.as_str();
    tracing::debug!("Writing configmap {name}");
    let oref = name_to_ostree_ref(&spec.name)?;
    let repo = &sysroot.repo();
    let tx = repo.auto_transaction(cancellable)?;
    let tree = &ostree::MutableTree::new();
    let dirmeta =
        create_and_commit_dirmeta(&repo, "/etc/some-unshipped-config-file".into(), sepolicy)?;
    {
        let serialized = serde_json::to_string(&cfgobj.config).context("Serializing")?;
        write_file(
            repo,
            tree,
            "config.json".into(),
            &dirmeta,
            serialized.as_bytes(),
            DEFAULT_MODE,
            sepolicy,
        )?;
    }
    let mut metadata = HashMap::new();
    let serialized_manifest =
        serde_json::to_string(&cfgobj.manifest).context("Serializing manifest")?;
    metadata.insert(CONFIGMAP_MANIFEST_KEY, serialized_manifest.to_variant());
    let timestamp = cfgobj
        .manifest
        .annotations()
        .as_ref()
        .and_then(|m| m.get(oci_spec::image::ANNOTATION_CREATED))
        .map(|v| chrono::DateTime::parse_from_rfc3339(v))
        .transpose()
        .context("Parsing created annotation")?
        .map(|t| t.timestamp() as u64)
        .unwrap_or_default();
    tracing::trace!("Writing commit with ts {timestamp}");

    let root = repo.write_mtree(&tree, cancellable)?;
    let root = root.downcast_ref::<ostree::RepoFile>().unwrap();
    let commit = repo.write_commit_with_time(
        None,
        None,
        None,
        Some(&metadata.to_variant()),
        root,
        timestamp,
        cancellable,
    )?;
    repo.transaction_set_ref(None, &oref, Some(commit.as_str()));
    tx.commit(cancellable)?;

    Ok(())
}

/// Parse a manifest, returning the single configmap descriptor (layer)
fn configmap_object_from_manifest(
    manifest: &oci_spec::image::ImageManifest,
) -> Result<&oci_spec::image::Descriptor> {
    let l = match manifest.layers().as_slice() {
        [] => anyhow::bail!("No layers in configmap manifest"),
        [l] => l,
        o => anyhow::bail!(
            "Expected exactly one layer in configmap manifest, found: {}",
            o.len()
        ),
    };
    match l.media_type() {
        oci_spec::image::MediaType::Other(o) if o.as_str() == MEDIA_TYPE_CONFIGMAP => Ok(l),
        o => anyhow::bail!("Expected media type {MEDIA_TYPE_CONFIGMAP} but found: {o}"),
    }
}

#[context("Fetching configmap from {imgref}")]
/// Download a configmap, honoring a previous manifest digest.  If the digest
/// hasn't changed, then this function will return None.
async fn fetch_configmap(
    proxy: &ImageProxy,
    imgref: &ostree_container::ImageReference,
    previous_manifest_digest: Option<&str>,
) -> Result<Option<Box<ConfigMapObject>>> {
    tracing::debug!("Fetching {imgref}");
    let imgref = imgref.to_string();
    let oimg = proxy.open_image(&imgref).await?;
    let (digest, manifest) = proxy.fetch_manifest(&oimg).await?;
    if previous_manifest_digest == Some(digest.as_str()) {
        return Ok(None);
    }
    let layer = configmap_object_from_manifest(&manifest)?;
    // Layer sizes shouldn't be negative
    let layer_size = u64::try_from(layer.size()).unwrap();
    let layer_size = u32::try_from(layer_size)?;
    if layer_size > CONFIGMAP_SIZE_LIMIT {
        anyhow::bail!(
            "configmap size limit is {CONFIGMAP_SIZE_LIMIT} bytes, found: {}",
            glib::format_size(layer_size.into())
        )
    }
    let (mut configmap_reader, driver) = proxy
        .get_blob(&oimg, layer.digest(), layer_size.into())
        .await?;
    let mut configmap_blob = String::new();
    let reader = configmap_reader.read_to_string(&mut configmap_blob);
    let (reader, driver) = tokio::join!(reader, driver);
    let _ = reader?;
    driver?;

    let config: ConfigMap = serde_json::from_str(&configmap_blob).context("Parsing configmap")?;
    Ok(Some(Box::new(ConfigMapObject { manifest, config })))
}

/// Download a configmap.
async fn fetch_required_configmap(
    proxy: &containers_image_proxy::ImageProxy,
    imgref: &ostree_container::ImageReference,
) -> Result<Box<ConfigMapObject>> {
    // SAFETY: We must get a new configmap here
    fetch_configmap(proxy, imgref, None)
        .await
        .map(|v| v.expect("internal error: expected configmap"))
}

/// Return the attached configmaps for a deployment.
#[context("Querying config names")]
pub(crate) fn configs_for_deployment(
    _sysroot: &SysrootLock,
    deployment: &Deployment,
) -> Result<Vec<ConfigSpec>> {
    let origin = deployment
        .origin()
        .ok_or_else(|| anyhow::anyhow!("Deployment is missing an origin"))?;
    origin
        .groups()
        .into_iter()
        .try_fold(Vec::new(), |mut acc, name| {
            let name = name.to_str();
            if let Some(name) = name.strip_prefix(ORIGIN_BOOTC_CONFIG_PREFIX) {
                let spec = ConfigSpec::from_keyfile(&origin, name)?;
                acc.push(spec);
            }
            anyhow::Ok(acc)
        })
}

async fn add(
    sysroot: &SysrootLock,
    imgref: &ostree_container::ImageReference,
    name: Option<&str>,
) -> Result<()> {
    let cancellable = gio::Cancellable::NONE;
    let repo = &sysroot.repo();
    let merge_deployment = &crate::cli::target_deployment(sysroot)?;
    let stateroot = merge_deployment.osname();
    let importer = new_proxy().await?;
    let cfgobj = fetch_required_configmap(&importer, imgref).await?;
    let origin = merge_deployment
        .origin()
        .ok_or_else(|| anyhow::anyhow!("Deployment is missing an origin"))?;
    let dirpath = sysroot.deployment_dirpath(merge_deployment);
    // SAFETY: None of this should be NULL
    let dirpath = sysroot.path().path().unwrap().join(dirpath);
    let deployment_fd = Dir::open_ambient_dir(&dirpath, cap_std::ambient_authority())
        .with_context(|| format!("Opening deployment directory {dirpath:?}"))?;
    let sepolicy = ostree::SePolicy::new_at(deployment_fd.as_raw_fd(), cancellable)?;
    let name = name
        .or_else(|| cfgobj.config.metadata.name.as_deref())
        .ok_or_else(|| anyhow!("Missing metadata.name and no name provided"))?;
    let configs = configs_for_deployment(sysroot, merge_deployment)?;
    if configs.iter().any(|v| v.name == name) {
        anyhow::bail!("Already have a config with name {name}");
    }
    let spec = ConfigSpec {
        name: name.to_owned(),
        imgref: imgref.clone(),
    };
    let oref = name_to_ostree_ref(name)?;
    tracing::trace!("configmap {name} => {oref}");
    // TODO use ostree_ext::tokio_util::spawn_blocking_cancellable_flatten(move |cancellable| {
    // once https://github.com/ostreedev/ostree/pull/2824 lands
    write_configmap(sysroot, Some(&sepolicy), &spec, &cfgobj, cancellable)?;
    println!("Stored configmap: {name}");

    spec.store(&origin);

    // let merge_commit = merge_deployment.csum();
    // let commit = require_base_commit(repo, &merge_commit)?;
    // let state = ostree_container::store::query_image_commit(repo, &commit)?;
    // crate::deploy::deploy(sysroot, Some(merge_deployment), &stateroot, state, &origin).await?;
    // crate::deploy::cleanup(sysroot).await?;
    // println!("Queued changes for next boot");

    Ok(())
}

async fn update_one_config(
    sysroot: &SysrootLock,
    merge_deployment: &ostree::Deployment,
    configs: &[&ConfigSpec],
    name: &str,
    proxy: &ImageProxy,
) -> Result<bool> {
    let cancellable = gio::Cancellable::NONE;
    let repo = &sysroot.repo();
    let cfgspec = configs
        .into_iter()
        .find(|v| v.name == name)
        .ok_or_else(|| anyhow::anyhow!("No config with name {name}"))?;
    let cfgref = cfgspec.ostree_ref()?;
    let cfg_commit = repo.require_rev(&cfgref)?;
    let cfg_commitv = repo.load_commit(&cfg_commit)?.0;
    let cfg_commitmeta = glib::VariantDict::new(Some(&cfg_commitv.child_value(0)));
    let etag = cfg_commitmeta
        .lookup::<String>(CONFIGMAP_ETAG_KEY)?
        .ok_or_else(|| anyhow!("Missing {CONFIGMAP_ETAG_KEY}"))?;
    let cfgobj = match fetch_configmap(proxy, &cfgspec.imgref, Some(etag.as_str())).await? {
        Some(v) => v,
        None => {
            return Ok(false);
        }
    };
    let dirpath = sysroot.deployment_dirpath(merge_deployment);
    // SAFETY: None of this should be NULL
    let dirpath = sysroot.path().path().unwrap().join(dirpath);
    let deployment_fd = Dir::open_ambient_dir(&dirpath, cap_std::ambient_authority())
        .with_context(|| format!("Opening deployment directory {dirpath:?}"))?;
    let sepolicy = ostree::SePolicy::new_at(deployment_fd.as_raw_fd(), cancellable)?;
    write_configmap(sysroot, Some(&sepolicy), cfgspec, &cfgobj, cancellable)?;
    Ok(true)
}

async fn update<S: AsRef<str>>(
    sysroot: &SysrootLock,
    names: impl Iterator<Item = S>,
) -> Result<()> {
    let proxy = &new_proxy().await?;
    let merge_deployment = &crate::cli::target_deployment(sysroot)?;
    let origin = merge_deployment
        .origin()
        .ok_or_else(|| anyhow::anyhow!("Deployment is missing an origin"))?;
    let configs = configs_for_deployment(sysroot, merge_deployment)?;
    let configs = configs.iter().collect::<Vec<_>>();
    let mut changed = false;
    for name in names {
        let name = name.as_ref();
        if update_one_config(sysroot, merge_deployment, configs.as_slice(), name, proxy).await? {
            println!("Updated configmap {name}");
            changed = true;
        } else {
            println!("No changes in configmap {name}");
        }
    }

    if !changed {
        return Ok(());
    }

    Ok(())
}
