use std::convert::Infallible;
use std::fs;
use std::io::Error;
use std::path::PathBuf;
use std::sync::Arc;
use async_trait::async_trait;
use hyper::{Body, body, Request, Response, Server};
use hyper::server::conn::AddrIncoming;
use hyper::service::{make_service_fn, service_fn};
use hyper_rustls::TlsAcceptor;
use slog::info;
use tokio::net::TcpListener;
use tokio::select;
use client::pkg::transport::listener::{new_tls_acceptor, TLSInfo};
use client::pkg::transport::transport::transport;
use raft::eraftpb::Message;
use raft::SnapshotStatus;
use crate::etcdserver::api::rafthttp::transport::{Raft, Transport};
use crate::etcdserver::api::rafthttp::util::util::write_and_sync_file;
use crate::etcdserver::async_ch::Channel;
use client::pkg::tlsutil::default_logger;

pub struct fakeRaft {
    pub recvc: Channel<Message>,
    pub err: String,
    pub removed_id: u64,
}



#[async_trait]
impl Raft for fakeRaft {
    async fn process(&self, m: Message) -> Result<(), Error> {
        select! {
                msg = self.recvc.recv() =>{},
            }
        return Err(Error::new(std::io::ErrorKind::Other, self.err.clone()));
    }

    fn is_id_removed(&self, id: u64) -> bool {
        return id == self.removed_id;
    }

    fn report_unreachable(&self, id: u64) {
        return;
    }

    fn report_snapshot(&self, id: u64, status: SnapshotStatus) {
        return;
    }
}

pub fn new_tr(url:String,info:TLSInfo) ->Transport{
    let tr = Transport::new(
        vec![url],
        None,
        std::time::Duration::from_secs(1),
        0.1,
        1,
        1,
        None,
        None,
        None,
        None,
        Option::from(transport(info.clone())),
        Option::from(Channel::new(1)),
        Arc::new(Box::new(fakeRaft {
            recvc: Channel::new(1),
            err: "error".to_string(),
            removed_id: 1,
        })),
    );
    tr
}

pub(crate) async fn server_succ(tlsinfo: TLSInfo) {

    let addr = "127.0.0.1:2380".parse().unwrap();
    let incoming = AddrIncoming::bind(&addr).unwrap();
    let tls_acceptor = new_tls_acceptor(tlsinfo.clone());
    let acceptor = TlsAcceptor::builder()
        .with_tls_config((*tlsinfo.clone().server_config()).clone())
        .with_all_versions_alpn()
        .with_incoming(incoming);
    let make_svc = make_service_fn(|_| {
        async {
            Ok::<_, Infallible>(service_fn(handler_succ))
        }
    });

    tokio::spawn(async move {
        let server = Server::builder(acceptor)
            .http2_only(true)
            .serve(make_svc);
        server.await.unwrap();
    });
    let listener = TcpListener::bind("127.0.0.1:2380").await.unwrap();

    info!(default_logger(),"server started");
}

pub async fn server_err(tlsinfo: TLSInfo) {
    let addr = "127.0.0.1:2380".parse().unwrap();
    let incoming = AddrIncoming::bind(&addr).unwrap();
    let tls_acceptor = new_tls_acceptor(tlsinfo.clone());
    let acceptor = TlsAcceptor::builder()
        .with_tls_config((*tlsinfo.clone().server_config()).clone())
        .with_all_versions_alpn()
        .with_incoming(incoming);

    let make_svc = make_service_fn(|_| {
        async {
            Ok::<_, Infallible>(service_fn(handler_err))
        }
    });

    tokio::spawn(async move {
        let server = Server::builder(acceptor)
            .http2_only(true)
            .serve(make_svc);
        server.await.unwrap();

    });
}

pub async fn handler_err(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let path = req.uri().path();
    match path {
        "/raft" => {
            let response = Response::builder()
                .status(403)
                .body(Body::from("error"))
                .unwrap();
            Ok(response)
        }
        "/raft/snapshot" =>{
            info!(default_logger(),"snapshot received");
            let response = Response::builder()
                .status(412)
                .header("X-Etcd-Cluster-ID",1)
                // .header("X-Etcd-Cluster-ID",1)
                .body(Body::from("Cluster ID mismatch"))
                .unwrap();

            Ok(response)
        }
        _ => {

            // 处理其他路径
            let response = Response::new(Body::from("Not found"));
            Ok(response)
        }
    }
}

pub async fn handler_succ(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let path = req.uri().path();
    match path {
        "/raft" => {
            let response = Response::builder()
                .status(204)
                .body(Body::from("error"))
                .unwrap();
            Ok(response)
        }
        "/raft/snapshot" =>{
            let req_body =  body::to_bytes(req.into_body()).await.unwrap();
            let mut dir = PathBuf::new();
            dir.push(std::env::temp_dir());
            dir.push("test-snap-send");
            fs::remove_dir_all(&dir).unwrap();
            fs::create_dir(&dir).unwrap();
            write_and_sync_file(&dir.join("snap.db"), &req_body.to_vec(), 0x66).await.unwrap();
            let response = Response::builder()
                .status(204)
                .body(Body::from("error"))
                .unwrap();
            Ok(response)
        }
        _ => {
            // 处理其他路径
            let response = Response::new(Body::from("Not found"));
            Ok(response)
        }
    }
}