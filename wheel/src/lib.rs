pub mod config;
pub mod error;
pub mod lsm_tree;
pub mod service;

use std::sync::Arc;

use bytesize::ByteSize;
use error::{config_err, err, Error, Result};
use lsm_tree::sstable_uploader::{SstableUploader, SstableUploaderOptions};
use lsm_tree::{WheelVersionManager, WheelVersionManagerOptions};
use runkv_proto::rudder::rudder_service_client::RudderServiceClient;
use runkv_proto::wheel::wheel_service_server::WheelServiceServer;
use runkv_storage::components::{BlockCache, SstableStore, SstableStoreOptions, SstableStoreRef};
use runkv_storage::manifest::{VersionManager, VersionManagerOptions};
use runkv_storage::{MemObjectStore, ObjectStoreRef, S3ObjectStore};
use service::{Wheel, WheelOptions};
use tonic::transport::{Channel, Server};
use tracing::info;

use crate::config::WheelConfig;
use crate::lsm_tree::{ObjectStoreLsmTree, ObjectStoreLsmTreeOptions};

pub async fn bootstrap_wheel(config: &WheelConfig, wheel: Wheel) -> Result<()> {
    let addr_str = format!("{}:{}", config.host, config.port);

    Server::builder()
        .add_service(WheelServiceServer::new(wheel))
        .serve(addr_str.parse().map_err(err)?)
        .await
        .map_err(err)
}

pub async fn build_wheel(config: &WheelConfig) -> Result<(Wheel, ObjectStoreLsmTree)> {
    let object_store = create_object_store(config).await;
    build_wheel_with_object_store(config, object_store).await
}

pub async fn build_wheel_with_object_store(
    config: &WheelConfig,
    object_store: ObjectStoreRef,
) -> Result<(Wheel, ObjectStoreLsmTree)> {
    let sstable_store = create_sstable_store(config, object_store)?;

    let version_manager = create_version_manager(config, sstable_store.clone())?;

    let lsm_tree = create_lsm_tree(config, sstable_store.clone(), version_manager.clone())?;

    let rudder_client = RudderServiceClient::connect(format!(
        "http://{}:{}",
        config.rudder.host, config.rudder.port
    ))
    .await?;

    let mut sstable_uploader = create_sstable_uploader(
        config,
        lsm_tree.clone(),
        sstable_store,
        version_manager.clone(),
        rudder_client.clone(),
    )?;

    let mut _wheel_version_manager =
        create_wheel_version_manager(config, version_manager, rudder_client)?;

    // TODO: Do not spawn when building.
    tokio::spawn(async move { sstable_uploader.run().await });
    // tokio::spawn(async move { wheel_version_manager.run().await });

    let options = WheelOptions {
        lsm_tree: lsm_tree.clone(),
        // sstable_uploader,
    };

    let wheel = Wheel::new(options);

    Ok((wheel, lsm_tree))
}

async fn create_object_store(config: &WheelConfig) -> ObjectStoreRef {
    if let Some(c) = &config.s3 {
        info!("s3 config found, create s3 object store");
        Arc::new(S3ObjectStore::new(c.bucket.clone()).await)
    } else if let Some(c) = &config.minio {
        info!("minio config found, create minio object store");
        Arc::new(S3ObjectStore::new_with_minio(&c.url).await)
    } else {
        info!("no object store config found, create default memory object store");
        Arc::new(MemObjectStore::default())
    }
}

fn create_sstable_store(
    config: &WheelConfig,
    object_store: ObjectStoreRef,
) -> Result<SstableStoreRef> {
    let block_cache = BlockCache::new(
        config
            .cache
            .block_cache_capacity
            .parse::<ByteSize>()
            .map_err(config_err)?
            .0 as usize,
    );
    let sstable_store_options = SstableStoreOptions {
        path: config.data_path.clone(),
        object_store,
        block_cache,
        meta_cache_capacity: config
            .cache
            .meta_cache_capacity
            .parse::<ByteSize>()
            .map_err(config_err)?
            .0 as usize,
    };
    let sstable_store = SstableStore::new(sstable_store_options);
    Ok(Arc::new(sstable_store))
}

fn create_version_manager(
    config: &WheelConfig,
    sstable_store: SstableStoreRef,
) -> Result<VersionManager> {
    let version_manager_options = VersionManagerOptions {
        levels_options: config.lsm_tree.levels_options.clone(),
        // TODO: Recover from meta or scanning.
        levels: vec![vec![]; config.lsm_tree.levels_options.len()],
        sstable_store,
    };
    Ok(VersionManager::new(version_manager_options))
}

fn create_lsm_tree(
    config: &WheelConfig,
    sstable_store: SstableStoreRef,
    version_manager: VersionManager,
) -> Result<ObjectStoreLsmTree> {
    let lsm_tree_options = ObjectStoreLsmTreeOptions {
        sstable_store,
        write_buffer_capacity: config
            .buffer
            .write_buffer_capacity
            .parse::<ByteSize>()
            .map_err(config_err)?
            .0 as usize,
        version_manager,
    };
    Ok(ObjectStoreLsmTree::new(lsm_tree_options))
}

fn create_sstable_uploader(
    config: &WheelConfig,
    lsm_tree: ObjectStoreLsmTree,
    sstable_store: SstableStoreRef,
    version_manager: VersionManager,
    rudder_client: RudderServiceClient<Channel>,
) -> Result<SstableUploader> {
    let sstable_uploader = SstableUploaderOptions {
        node_id: config.id,
        lsm_tree,
        sstable_store,
        version_manager,
        sstable_capacity: config
            .lsm_tree
            .sstable_capacity
            .parse::<ByteSize>()
            .map_err(config_err)?
            .0 as usize,
        block_capacity: config
            .lsm_tree
            .block_capacity
            .parse::<ByteSize>()
            .map_err(config_err)?
            .0 as usize,
        restart_interval: config.lsm_tree.restart_interval,
        bloom_false_positive: config.lsm_tree.bloom_false_positive,
        compression_algorithm: config
            .lsm_tree
            .levels_options
            .get(0)
            .ok_or_else(|| Error::Other("no L0 in lsm_tree.levels_options".to_string()))?
            .compression_algorithm,
        poll_interval: config
            .poll_interval
            .parse::<humantime::Duration>()
            .map_err(config_err)?
            .into(),
        rudder_client,
    };
    Ok(SstableUploader::new(sstable_uploader))
}

fn create_wheel_version_manager(
    config: &WheelConfig,
    version_manager: VersionManager,
    rudder_client: RudderServiceClient<Channel>,
) -> Result<WheelVersionManager> {
    let wheel_version_manager_options = WheelVersionManagerOptions {
        node_id: config.id,
        version_manager,
        client: rudder_client,
        heartbeat_interval: config
            .heartbeat_interval
            .parse::<humantime::Duration>()
            .map_err(config_err)?
            .into(),
    };
    Ok(WheelVersionManager::new(wheel_version_manager_options))
}
