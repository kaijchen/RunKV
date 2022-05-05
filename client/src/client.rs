#![allow(dead_code)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use runkv_common::notify_pool::NotifyPool;
use runkv_common::Worker;
use runkv_proto::kv::stream_kv_service_client::StreamKvServiceClient;
use runkv_proto::kv::{
    kv_op_request, kv_op_response, DeleteRequest, GetRequest, KvOpRequest, PutRequest,
    StreamTxnRequest, StreamTxnResponse,
};
use tokio::sync::mpsc;
use tonic::transport::Channel;
use tonic::Request;
use tracing::error;

use crate::error::{Error, Result};

pub type Snapshot = u64;

#[async_trait]
pub trait RunKvClient: Send + Sync + Clone + 'static {
    async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>>;
    async fn snapshot_get(&self, key: Vec<u8>, snapshot: Snapshot) -> Result<Option<Vec<u8>>>;
    async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;
    async fn delete(&self, key: Vec<u8>) -> Result<()>;
    async fn snapshot(&self) -> Result<Snapshot>;
}

struct GrpcRunKVClientWorker {
    rx: Option<mpsc::UnboundedReceiver<StreamTxnRequest>>,
    grpc_client: StreamKvServiceClient<Channel>,
    notify_pool: NotifyPool<u64, StreamTxnResponse>,
}

#[async_trait]
impl Worker for GrpcRunKVClientWorker {
    async fn run(&mut self) -> anyhow::Result<()> {
        self.run_inner().await.map_err(|e| e.into())
    }
}

impl GrpcRunKVClientWorker {
    async fn run_inner(&mut self) -> Result<()> {
        // TODO: Gracefully kill.
        let mut rx = self.rx.take().unwrap();
        let input = async_stream::stream! {
            while let Some(req) = rx.recv().await {
                yield req;
            }
        };

        let response = self.grpc_client.stream_txn(Request::new(input)).await?;
        let mut output = response.into_inner();

        while let Some(rsp) = output.message().await? {
            let id = rsp.id;
            self.notify_pool.notify(id, rsp).map_err(Error::err)?;
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct GrpcRunKVClient {
    client_id: u64,
    tx: mpsc::UnboundedSender<StreamTxnRequest>,
    id: Arc<AtomicU64>,
    notify_pool: NotifyPool<u64, StreamTxnResponse>,
}

impl GrpcRunKVClient {
    // TODO: Use rudder service name insteal of channel. RunKvClient should resolve router with
    // rudder itself.
    async fn open(client_id: u64, channel: Channel) -> Self {
        let grpc_client = StreamKvServiceClient::new(channel);
        let (tx, rx) = mpsc::unbounded_channel();
        let notify_pool = NotifyPool::default();

        let mut worker = GrpcRunKVClientWorker {
            rx: Some(rx),
            grpc_client,
            notify_pool: notify_pool.clone(),
        };
        // TODO: Hold the handle for gracefully shutdown.
        let _handle = tokio::spawn(async move {
            if let Err(e) = worker.run().await {
                error!("error raised when running runkv client worker: {}", e);
            }
        });

        Self {
            client_id,
            tx,
            id: Arc::new(AtomicU64::new(0)),
            notify_pool,
        }
    }

    async fn get_inner(&self, key: Vec<u8>, sequence: u64) -> Result<Option<Vec<u8>>> {
        let id = self.id.fetch_add(1, Ordering::SeqCst) + 1;
        let req = StreamTxnRequest {
            client_id: self.client_id,
            id,
            ops: vec![KvOpRequest {
                request: Some(kv_op_request::Request::Get(GetRequest { key, sequence })),
            }],
        };
        let notify = self.notify_pool.register(id).map_err(Error::err)?;
        self.tx.send(req).map_err(Error::err)?;
        let mut rsp = notify.await.map_err(Error::err)?;
        let value = match rsp.ops.remove(0).response.unwrap() {
            kv_op_response::Response::Get(get) => get.value,
            _ => unreachable!(),
        };
        let value = if value.is_empty() { None } else { Some(value) };
        Ok(value)
    }

    async fn put_inner(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let id = self.id.fetch_add(1, Ordering::SeqCst) + 1;
        let req = StreamTxnRequest {
            client_id: self.client_id,
            id,
            ops: vec![KvOpRequest {
                request: Some(kv_op_request::Request::Put(PutRequest { key, value })),
            }],
        };
        let notify = self.notify_pool.register(id).map_err(Error::err)?;
        self.tx.send(req).map_err(Error::err)?;
        let mut rsp = notify.await.map_err(Error::err)?;
        match rsp.ops.remove(0).response.unwrap() {
            kv_op_response::Response::Put(_) => {}
            _ => unreachable!(),
        };
        Ok(())
    }

    async fn delete_inner(&self, key: Vec<u8>) -> Result<()> {
        let id = self.id.fetch_add(1, Ordering::SeqCst) + 1;
        let req = StreamTxnRequest {
            client_id: self.client_id,
            id,
            ops: vec![KvOpRequest {
                request: Some(kv_op_request::Request::Delete(DeleteRequest { key })),
            }],
        };
        let notify = self.notify_pool.register(id).map_err(Error::err)?;
        self.tx.send(req).map_err(Error::err)?;
        let mut rsp = notify.await.map_err(Error::err)?;
        match rsp.ops.remove(0).response.unwrap() {
            kv_op_response::Response::Delete(_) => {}
            _ => unreachable!(),
        };
        Ok(())
    }
}

#[async_trait]
impl RunKvClient for GrpcRunKVClient {
    async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        self.get_inner(key, u64::MAX).await
    }

    async fn snapshot_get(&self, key: Vec<u8>, snapshot: Snapshot) -> Result<Option<Vec<u8>>> {
        self.get_inner(key, snapshot).await
    }

    async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.put_inner(key, value).await
    }

    async fn delete(&self, key: Vec<u8>) -> Result<()> {
        self.delete_inner(key).await
    }

    async fn snapshot(&self) -> Result<Snapshot> {
        todo!()
    }
}
