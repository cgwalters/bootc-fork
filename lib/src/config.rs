use std::collections::HashMap;
use std::io::Read;

use crate::deploy::RequiredHostSpec;
use crate::k8sapitypes::ConfigMap;
use anyhow::{anyhow, Context, Result};
use camino::Utf8Path;
use cap_std_ext::cap_std;
use containers_image_proxy::ImageProxy;
use fn_error_context::context;
use ostree_ext::container as ostree_container;
use ostree_ext::oci_spec;
use ostree_ext::prelude::{Cast, FileExt, InputStreamExtManual, ToVariant};
use ostree_ext::{gio, glib, ostree};
use ostree_ext::{ostree::Deployment, sysroot::SysrootLock};
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

/// The location to find updates
const CONFIGMAP_SOURCE_KEY: &str = "bootc.configmap.imgref";
/// The key used to store the manifest
const CONFIGMAP_MANIFEST_KEY: &str = "bootc.configmap.manifest";
/// The key used to store the manifest digest
const CONFIGMAP_MANIFEST_DIGEST_KEY: &str = "bootc.configmap.digest";

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
    manifest_digest: String,
    imgref: Option<String>,
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

async fn show(sysroot: &SysrootLock, name: &str) -> Result<()> {
    let cancellable = gio::Cancellable::NONE;
    let oref = &name_to_ostree_ref(name)?;
    let config = read_configmap_data(&sysroot.repo(), oref, cancellable)?;
    let mut stdout = std::io::stdout().lock();
    serde_yaml::to_writer(&mut stdout, &config.config)?;
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
    booted_deployment: &ostree::Deployment,
    name: &str,
    cfgobj: &ConfigMapObject,
    cancellable: Option<&gio::Cancellable>,
) -> Result<()> {
    use crate::ostree_generation::{create_and_commit_dirmeta, write_file};

    let repo = &sysroot.repo();
    let sepolicy =
        ostree::SePolicy::from_commit(repo, booted_deployment.csum().as_str(), cancellable)?;
    let sepolicy = Some(&sepolicy);
    tracing::debug!("Writing configmap {name}");
    let oref = name_to_ostree_ref(name)?;
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

#[context("Reading configmap")]
fn read_configmap_data(
    repo: &ostree::Repo,
    rev: &str,
    cancellable: Option<&gio::Cancellable>,
) -> Result<ConfigMapObject> {
    let (root, rev) = repo.read_commit(rev, cancellable)?;
    let reader = root.child("config.json").read(cancellable)?;
    let mut reader = reader.into_read();
    let config = serde_json::from_reader(&mut reader).context("Parsing config.json")?;
    let commitv = repo.load_commit(&rev)?.0;
    let commitmeta = &glib::VariantDict::new(Some(&commitv.child_value(0)));
    let manifest_bytes = commitmeta
        .lookup::<String>(CONFIGMAP_MANIFEST_KEY)?
        .ok_or_else(|| anyhow!("Missing metadata key {CONFIGMAP_MANIFEST_KEY}"))?;
    let manifest = serde_json::from_str(&manifest_bytes).context("Parsing manifest")?;
    let manifest_digest = commitmeta
        .lookup::<String>(CONFIGMAP_MANIFEST_DIGEST_KEY)?
        .ok_or_else(|| anyhow!("Missing metadata key {CONFIGMAP_MANIFEST_DIGEST_KEY}"))?;
    let imgref = commitmeta.lookup::<String>(CONFIGMAP_SOURCE_KEY)?;
    Ok(ConfigMapObject {
        manifest,
        manifest_digest,
        imgref,
        config,
    })
}

#[context("Applying configmap")]
pub(crate) fn apply_configmap(
    repo: &ostree::Repo,
    root: &ostree::MutableTree,
    sepolicy: Option<&ostree::SePolicy>,
    name: &str,
    cancellable: Option<&gio::Cancellable>,
) -> Result<()> {
    let oref = name_to_ostree_ref(name)?;
    let mapobj = &read_configmap_data(repo, &oref, cancellable)?;
    let map = &mapobj.config;
    let dirmeta = crate::ostree_generation::create_and_commit_dirmeta(
        repo,
        "/etc/some-unshipped-config-file".into(),
        sepolicy,
    )?;
    // Create an iterator over the string data
    let string_data = map.data.iter().flatten().map(|(k, v)| (k, v.as_bytes()));
    // Create an iterator over the binary data
    let binary_data = map
        .binary_data
        .iter()
        .flatten()
        .map(|(k, v)| (k, v.0.as_slice()));
    let prefix = get_prefix(map);
    tracing::trace!("prefix={prefix}");
    // For each string and binary value, write a file
    let mut has_content = false;
    for (k, v) in string_data.chain(binary_data) {
        let path = Utf8Path::new(prefix).join(k);
        tracing::trace!("Writing {path}");
        crate::ostree_generation::write_file(
            repo,
            root,
            &path,
            &dirmeta,
            v,
            DEFAULT_MODE,
            sepolicy,
        )?;
        has_content = true;
    }
    if !has_content {
        anyhow::bail!("ConfigMap has no data");
    }
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
    let (manifest_digest, manifest) = proxy.fetch_manifest(&oimg).await?;
    if previous_manifest_digest == Some(manifest_digest.as_str()) {
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
    Ok(Some(Box::new(ConfigMapObject {
        manifest,
        manifest_digest,
        imgref: imgref.to_string().into(),
        config,
    })))
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
    let (booted_deployment, _deployments, host) =
        crate::status::get_status_require_booted(sysroot)?;
    let spec = RequiredHostSpec::from_spec(&host.spec)?;
    let repo = &sysroot.repo();
    let importer = new_proxy().await?;
    let cfgobj = fetch_required_configmap(&importer, imgref).await?;
    let name = name
        .or_else(|| cfgobj.config.metadata.name.as_deref())
        .ok_or_else(|| anyhow!("Missing metadata.name and no name provided"))?;
    if spec.configmaps.iter().any(|v| v == name) {
        anyhow::bail!("Config with name '{name}' already attached");
    }
    let oref = name_to_ostree_ref(name)?;
    tracing::trace!("configmap {name} => {oref}");
    // TODO use ostree_ext::tokio_util::spawn_blocking_cancellable_flatten(move |cancellable| {
    // once https://github.com/ostreedev/ostree/pull/2824 lands
    write_configmap(sysroot, &booted_deployment, name, &cfgobj, cancellable)?;
    println!("Stored configmap: {name}");

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
    todo!()
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
