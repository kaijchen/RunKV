use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use lazy_static::lazy_static;
use runkv_common::channel_pool::ChannelPool;
use runkv_common::coding::BytesSerde;
use runkv_common::config::Node;
use runkv_common::context::Context;
use runkv_common::notify_pool::NotifyPool;
use runkv_proto::common::Endpoint;
use runkv_proto::kv::kv_service_server::KvService;
use runkv_proto::kv::stream_kv_service_server::StreamKvService;
use runkv_proto::kv::{
    kv_op_request, kv_op_response, DeleteRequest, DeleteResponse, GetRequest, GetResponse,
    KvOpRequest, PutRequest, PutResponse, SnapshotRequest, SnapshotResponse, StreamTxnRequest,
    StreamTxnResponse, TxnRequest, TxnResponse,
};
use runkv_proto::wheel::raft_service_server::RaftService;
use runkv_proto::wheel::wheel_service_server::WheelService;
use runkv_proto::wheel::{
    AddEndpointsRequest, AddEndpointsResponse, AddKeyRangeRequest, AddKeyRangeResponse,
    RaftRequest, RaftResponse,
};
use tonic::{Request, Response, Status, Streaming};
use tracing::{trace, trace_span, Instrument};

use crate::components::command::Command;
use crate::components::raft_manager::RaftManager;
use crate::components::raft_network::{GrpcRaftNetwork, RaftNetwork};
use crate::error::{Error, KvError, Result};
use crate::meta::MetaStoreRef;
use crate::worker::raft::Proposal;

lazy_static! {
    static ref KV_SERVICE_GET_LATENCY_HISTOGRAM_VEC: prometheus::HistogramVec =
        prometheus::register_histogram_vec!(
            "kv_service_get_latency_histogram_vec",
            "kv service get latency histogram vec",
            &["node"]
        )
        .unwrap();
    static ref KV_SERVICE_PUT_LATENCY_HISTOGRAM_VEC: prometheus::HistogramVec =
        prometheus::register_histogram_vec!(
            "kv_service_put_latency_histogram_vec",
            "kv service put latency histogram vec",
            &["node"]
        )
        .unwrap();
    static ref KV_SERVICE_DELETE_LATENCY_HISTOGRAM_VEC: prometheus::HistogramVec =
        prometheus::register_histogram_vec!(
            "kv_service_delete_latency_histogram_vec",
            "kv service delete latency histogram vec",
            &["node"]
        )
        .unwrap();
    static ref KV_SERVICE_SNAPSHOT_LATENCY_HISTOGRAM_VEC: prometheus::HistogramVec =
        prometheus::register_histogram_vec!(
            "kv_service_snapshot_latency_histogram_vec",
            "kv service snapshot latency histogram vec",
            &["node"]
        )
        .unwrap();
    static ref KV_SERVICE_TXN_LATENCY_HISTOGRAM_VEC: prometheus::HistogramVec =
        prometheus::register_histogram_vec!(
            "kv_service_txn_latency_histogram_vec",
            "kv service txn latency histogram vec",
            &["node"]
        )
        .unwrap();
}

struct WheelServiceMetrics {
    kv_service_get_latency_histogram_vec: prometheus::Histogram,
    kv_service_put_latency_histogram_vec: prometheus::Histogram,
    kv_service_delete_latency_histogram_vec: prometheus::Histogram,
    kv_service_snapshot_latency_histogram_vec: prometheus::Histogram,
    kv_service_txn_latency_histogram_vec: prometheus::Histogram,
}

impl WheelServiceMetrics {
    fn new(node: u64) -> Self {
        Self {
            kv_service_get_latency_histogram_vec: KV_SERVICE_GET_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&[&node.to_string()])
                .unwrap(),
            kv_service_put_latency_histogram_vec: KV_SERVICE_PUT_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&[&node.to_string()])
                .unwrap(),
            kv_service_delete_latency_histogram_vec: KV_SERVICE_DELETE_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&[&node.to_string()])
                .unwrap(),
            kv_service_snapshot_latency_histogram_vec: KV_SERVICE_SNAPSHOT_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&[&node.to_string()])
                .unwrap(),
            kv_service_txn_latency_histogram_vec: KV_SERVICE_TXN_LATENCY_HISTOGRAM_VEC
                .get_metric_with_label_values(&[&node.to_string()])
                .unwrap(),
        }
    }
}

fn internal(e: impl Into<Box<dyn std::error::Error>>) -> Status {
    Status::internal(e.into().to_string())
}

pub struct WheelOptions {
    pub node: u64,
    pub meta_store: MetaStoreRef,
    pub channel_pool: ChannelPool,
    pub raft_network: GrpcRaftNetwork,
    pub raft_manager: RaftManager,
    pub txn_notify_pool: NotifyPool<u64, Result<TxnResponse>>,
}

struct WheelInner {
    meta_store: MetaStoreRef,
    channel_pool: ChannelPool,
    raft_network: GrpcRaftNetwork,
    raft_manager: RaftManager,
    txn_notify_pool: NotifyPool<u64, Result<TxnResponse>>,
    request_id: AtomicU64,

    metrics: WheelServiceMetrics,
}

#[derive(Clone)]
pub struct Wheel {
    node: u64,
    inner: Arc<WheelInner>,
}

impl Wheel {
    pub fn new(options: WheelOptions) -> Self {
        Self {
            node: options.node,
            inner: Arc::new(WheelInner {
                meta_store: options.meta_store,
                channel_pool: options.channel_pool,
                raft_network: options.raft_network,
                raft_manager: options.raft_manager,
                txn_notify_pool: options.txn_notify_pool,
                request_id: AtomicU64::new(0),

                metrics: WheelServiceMetrics::new(options.node),
            }),
        }
    }

    pub async fn prometheus_service(
        _request: http::Request<hyper::Body>,
    ) -> Result<http::Response<hyper::Body>> {
        use prometheus::Encoder;

        let encoder = prometheus::TextEncoder::new();
        let mut buffer = vec![];

        let metric_families = prometheus::gather();

        encoder.encode(&metric_families, &mut buffer).unwrap();
        let response = hyper::Response::builder()
            .status(200)
            .header(hyper::header::CONTENT_TYPE, encoder.format_type())
            .body(hyper::Body::from(buffer))
            .unwrap();
        Ok(response)
    }
}

impl std::fmt::Debug for Wheel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wheel").field("node", &self.node).finish()
    }
}

impl Wheel {
    async fn get_inner(&self, request: GetRequest) -> Result<GetResponse> {
        let req = TxnRequest {
            ops: vec![KvOpRequest {
                request: Some(kv_op_request::Request::Get(request)),
            }],
        };
        let mut rsp = self.txn_inner(req).await?;
        let response = match rsp.ops.remove(0).response.unwrap() {
            kv_op_response::Response::Get(response) => response,
            _ => unreachable!(),
        };
        Ok(response)
    }

    async fn put_inner(&self, request: PutRequest) -> Result<PutResponse> {
        let req = TxnRequest {
            ops: vec![KvOpRequest {
                request: Some(kv_op_request::Request::Put(request)),
            }],
        };
        let mut rsp = self.txn_inner(req).await?;
        let response = match rsp.ops.remove(0).response.unwrap() {
            kv_op_response::Response::Put(response) => response,
            _ => unreachable!(),
        };
        Ok(response)
    }

    async fn delete_inner(&self, request: DeleteRequest) -> Result<DeleteResponse> {
        let req = TxnRequest {
            ops: vec![KvOpRequest {
                request: Some(kv_op_request::Request::Delete(request)),
            }],
        };
        let mut rsp = self.txn_inner(req).await?;
        let response = match rsp.ops.remove(0).response.unwrap() {
            kv_op_response::Response::Delete(response) => response,
            _ => unreachable!(),
        };
        Ok(response)
    }

    async fn snapshot_inner(&self, request: SnapshotRequest) -> Result<SnapshotResponse> {
        let req = TxnRequest {
            ops: vec![KvOpRequest {
                request: Some(kv_op_request::Request::Snapshot(request)),
            }],
        };
        let mut rsp = self.txn_inner(req).await?;
        let response = match rsp.ops.remove(0).response.unwrap() {
            kv_op_response::Response::Snapshot(response) => response,
            _ => unreachable!(),
        };
        Ok(response)
    }

    #[tracing::instrument(level = "trace", fields(request_id))]
    async fn txn_inner(&self, request: TxnRequest) -> Result<TxnResponse> {
        let now = std::time::Instant::now();
        let span = tracing::Span::current();
        let span_id = span.id();
        // Pick raft leader of the request.
        let raft_nodes = self.txn_raft_nodes(&request).await?;
        assert!(!raft_nodes.is_empty());
        let raft_node = *raft_nodes.first().unwrap();

        let read_only = request.ops.iter().all(|op| {
            matches!(
                op.request,
                Some(kv_op_request::Request::Snapshot(_)) | Some(kv_op_request::Request::Get(_))
            )
        });
        let sequence = self.inner.raft_manager.get_sequence(raft_node).await?;
        let sequence = if read_only {
            sequence.load(Ordering::Acquire)
        } else {
            sequence.fetch_add(1, Ordering::SeqCst) + 1
        };

        // Register request.
        let request_id = self.inner.request_id.fetch_add(1, Ordering::SeqCst) + 1;
        span.record("request_id", &request_id);
        let rx = self
            .inner
            .txn_notify_pool
            .register(request_id)
            .map_err(Error::err)?;

        // Propose cmd with raft leader.
        let cmd = Command::TxnRequest {
            request_id,
            sequence,
            request,
        };
        let ctx = Context {
            span_id: span_id.map_or(0, |id| id.into_u64()),
            request_id,
        };
        let data = cmd.encode_to_vec().map_err(Error::serde_err)?;
        let context = ctx.encode_to_vec().map_err(Error::serde_err)?;

        let proposal_tx = self
            .inner
            .raft_manager
            .get_proposal_channel(raft_node)
            .await?;

        proposal_tx
            .send(Proposal { data, context })
            .map_err(Error::err)?;

        // Wait for resposne.
        let response = rx
            .instrument(trace_span!("wait_apply"))
            .await
            .map_err(Error::err)?;
        tracing::info!("txn inner takes: {:?}", now.elapsed());
        response
    }

    async fn txn_raft_nodes<'a>(&self, request: &'a TxnRequest) -> Result<Vec<u64>> {
        assert!(!request.ops.is_empty());

        let key = |req: &'a kv_op_request::Request| -> &'a [u8] {
            match req {
                kv_op_request::Request::Get(GetRequest { key, .. }) => key,
                kv_op_request::Request::Put(PutRequest { key, .. }) => key,
                kv_op_request::Request::Delete(DeleteRequest { key }) => key,
                kv_op_request::Request::Snapshot(SnapshotRequest { key }) => key,
            }
        };

        let keys = request
            .ops
            .iter()
            .map(|op| key(op.request.as_ref().unwrap()))
            .collect_vec();

        let (_range, _group, raft_nodes) = self
            .inner
            .meta_store
            .all_in_range(&keys)
            .await?
            .ok_or_else(|| KvError::InvalidShard(format!("request {:?}", request)))?;

        // TODO: Find the potential leader.
        Ok(raft_nodes)
    }

    async fn stream_txn_raft_nodes<'a>(&self, request: &'a StreamTxnRequest) -> Result<Vec<u64>> {
        assert!(!request.ops.is_empty());

        let key = |req: &'a kv_op_request::Request| -> &'a [u8] {
            match req {
                kv_op_request::Request::Get(GetRequest { key, .. }) => key,
                kv_op_request::Request::Put(PutRequest { key, .. }) => key,
                kv_op_request::Request::Delete(DeleteRequest { key }) => key,
                kv_op_request::Request::Snapshot(SnapshotRequest { key }) => key,
            }
        };

        let keys = request
            .ops
            .iter()
            .map(|op| key(op.request.as_ref().unwrap()))
            .collect_vec();

        let (_range, _group, raft_nodes) = self
            .inner
            .meta_store
            .all_in_range(&keys)
            .await?
            .ok_or_else(|| KvError::InvalidShard(format!("request {:?}", request)))?;

        // TODO: Find the potential leader.
        Ok(raft_nodes)
    }
}

#[async_trait]
impl WheelService for Wheel {
    #[tracing::instrument(level = "trace")]
    async fn add_endpoints(
        &self,
        request: Request<AddEndpointsRequest>,
    ) -> core::result::Result<Response<AddEndpointsResponse>, Status> {
        let req = request.into_inner();
        for (node, Endpoint { host, port }) in req.endpoints.iter() {
            let node = Node {
                id: *node,
                host: host.to_owned(),
                port: *port as u16,
            };
            self.inner.channel_pool.put_node(node).await;
        }
        Ok(Response::new(AddEndpointsResponse::default()))
    }

    #[tracing::instrument(level = "trace")]
    async fn add_key_range(
        &self,
        request: Request<AddKeyRangeRequest>,
    ) -> core::result::Result<Response<AddKeyRangeResponse>, Status> {
        let req = request.into_inner();
        self.inner
            .meta_store
            .add_key_range(req.key_range.unwrap(), req.group, &req.raft_nodes)
            .await
            .map_err(internal)?;

        self.inner
            .raft_network
            .register(
                req.group,
                BTreeMap::from_iter(
                    req.nodes
                        .iter()
                        .map(|(&raft_node, &node)| (raft_node, node)),
                ),
            )
            .await
            .map_err(internal)?;

        for raft_node in req.raft_nodes.iter() {
            self.inner
                .raft_manager
                .create_raft_node(req.group, *raft_node)
                .await
                .map_err(internal)?;
        }

        let rsp = AddKeyRangeResponse::default();
        Ok(Response::new(rsp))
    }
}

#[async_trait]
impl RaftService for Wheel {
    async fn raft(
        &self,
        request: Request<Streaming<RaftRequest>>,
    ) -> core::result::Result<Response<RaftResponse>, Status> {
        let mut stream = request.into_inner();

        while let Some(request) = stream.next().await {
            let req = request?;
            let msg = bincode::deserialize(&req.data)
                .map_err(Error::serde_err)
                .map_err(internal)?;
            self.inner
                .raft_network
                .recv(vec![msg])
                .await
                .map_err(internal)?;
        }

        Ok(Response::new(RaftResponse::default()))
    }
}

#[async_trait]
impl KvService for Wheel {
    #[tracing::instrument(level = "trace")]
    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> core::result::Result<Response<GetResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let rsp = self.get_inner(req).await.map_err(internal)?;
        let elapsed = start.elapsed();
        self.inner
            .metrics
            .kv_service_get_latency_histogram_vec
            .observe(elapsed.as_secs_f64());
        Ok(Response::new(rsp))
    }

    #[tracing::instrument(level = "trace")]
    async fn put(
        &self,
        request: Request<PutRequest>,
    ) -> core::result::Result<Response<PutResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let rsp = self.put_inner(req).await.map_err(internal)?;
        let elapsed = start.elapsed();
        self.inner
            .metrics
            .kv_service_put_latency_histogram_vec
            .observe(elapsed.as_secs_f64());
        Ok(Response::new(rsp))
    }

    #[tracing::instrument(level = "trace")]
    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> core::result::Result<Response<DeleteResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let rsp = self.delete_inner(req).await.map_err(internal)?;

        let elapsed = start.elapsed();
        self.inner
            .metrics
            .kv_service_delete_latency_histogram_vec
            .observe(elapsed.as_secs_f64());
        Ok(Response::new(rsp))
    }

    #[tracing::instrument(level = "trace")]
    async fn snapshot(
        &self,
        request: Request<SnapshotRequest>,
    ) -> core::result::Result<Response<SnapshotResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let rsp = self.snapshot_inner(req).await.map_err(internal)?;
        let elapsed = start.elapsed();
        self.inner
            .metrics
            .kv_service_snapshot_latency_histogram_vec
            .observe(elapsed.as_secs_f64());
        Ok(Response::new(rsp))
    }

    #[tracing::instrument(level = "trace")]
    async fn txn(
        &self,
        request: Request<TxnRequest>,
    ) -> core::result::Result<Response<TxnResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let rsp = self.txn_inner(req).await.map_err(internal)?;
        let elapsed = start.elapsed();
        self.inner
            .metrics
            .kv_service_txn_latency_histogram_vec
            .observe(elapsed.as_secs_f64());
        Ok(Response::new(rsp))
    }
}

#[async_trait]
impl StreamKvService for Wheel {
    type StreamTxnStream = Pin<
        Box<dyn Stream<Item = core::result::Result<StreamTxnResponse, Status>> + Send + 'static>,
    >;

    async fn stream_txn(
        &self,
        request: Request<Streaming<StreamTxnRequest>>,
    ) -> core::result::Result<Response<Self::StreamTxnStream>, Status> {
        let mut input = request.into_inner();

        // let wheel = self.clone();

        let output = async_stream::try_stream! {
            while let Some(request) = input.next().await {
                let req = request?;
                let client_id = req.client_id;
                trace!("req: {:?}, client id: {}",req,client_id);
                // let raft_nodes = wheel.stream_txn_raft_nodes(&req).await.map_err(internal)?;
                // let raft_node = raft_nodes[0];
                let rsp = StreamTxnResponse::default();
                yield rsp;
            }
        };

        Ok(Response::new(Box::pin(output) as Self::StreamTxnStream))
    }
}
