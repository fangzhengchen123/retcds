use std::io::Error;
use std::sync::{Arc, Mutex};
use chrono::Local;
use humanize_bytes::humanize_bytes_binary;
use hyper::{Body, Request, Response};
use crate::etcdserver::api::rafthttp::peer_status::{FailureType, PeerStatus};
use crate::etcdserver::api::rafthttp::snap::message::SnapMessage;
use crate::etcdserver::api::rafthttp::transport::{Raft, Transport};
use crate::etcdserver::api::rafthttp::types::id::ID;
use crate::etcdserver::api::rafthttp::url_pick::urlPicker;
use crate::etcdserver::async_ch::Channel;
use protobuf::Message as protoMessage;
use slog::{info, warn};
use tokio::select;
use crate::etcdserver::api::rafthttp::http::{RaftPrefix, RaftSnapshotPrefix};
use crate::etcdserver::api::rafthttp::util::net_util::{check_post_response, create_POST_request, CustomError};
use crate::etcdserver::api::rafthttp::default_logger;
use crate::etcdserver::api::rafthttp::peer::sendSnap;



pub struct snapshotSender{
    from : ID,
    to : ID,
    cid : ID,

    tr : Arc<Mutex<Transport>>,
    picker : urlPicker,
    status : Arc<Mutex<PeerStatus>>,
    raft: Arc<Box<dyn Raft + Send + Sync>>,

    errorc : Option<Channel<Error>>,

    stopc : Channel<()>,
}

pub fn new_snapshot_sender(tr: Transport,
                           picker: urlPicker,
                           status: Arc<Mutex<PeerStatus>>,
                           to:ID) -> snapshotSender {
    snapshotSender {
        from: tr.get_id(),
        to,
        cid: tr.get_cluster_id().clone(),
        tr : Arc::new(Mutex::new(tr.clone())),
        picker,
        status,
        raft:tr.get_raft().clone(),
        errorc: tr.get_errorc().clone(),
        stopc: Channel::new(1),
    }
}


impl snapshotSender{

    pub async fn stop(&self){
        self.stopc.send(()).await.expect("failed to send stop signal");
    }

    pub async fn send(&mut self, merged:SnapMessage){

        let m = merged.clone().get_msg();
        let body =  create_snap_body(merged.clone());
        let u = self.picker.get_base_url_picker().lock().unwrap().pick().unwrap();

        let req = create_POST_request(u.clone(),RaftSnapshotPrefix,body.clone(),"application/octet-stream",self.tr.lock().unwrap().get_urls(),self.from,self.cid).expect("failed to create post request");
        let req_check = create_POST_request(u.clone(),RaftSnapshotPrefix,body.clone(),"application/octet-stream",self.tr.lock().unwrap().get_urls(),self.from,self.cid).expect("failed to create post request");

        let snapshotSizeVal = merged.clone().get_total_size();
        let snapshotSize=humanize_bytes_binary!(snapshotSizeVal.clone());
        info!(default_logger(),"sending database snapshot snapshot-index =>{} remote-peer-id=>{} bytes=>{}  size =>{}",
            m.get_snapshot().get_metadata().get_index(),
            m.get_to(),
            snapshotSizeVal,
            snapshotSize.to_string());

        let res =  self.post(req,req_check).await;
        if res.is_err(){
            warn!(default_logger(),"failed to send database snapshot snapshot-index =>{} remote-peer-id=>{} bytes=>{}  size =>{}",
            m.get_snapshot().get_metadata().get_index(),
            m.get_to(),
            snapshotSizeVal,
            snapshotSize.clone().as_str());

            let err = res.err().unwrap();
            let str =  err.to_string().clone();
            if str == CustomError::MemberRemoved.to_string(){
                self.errorc.clone().unwrap().send(err).await.expect("failed to send error");
            }
            self.picker.get_base_url_picker().lock().unwrap().unreachable(u.clone());;
            self.status.lock().unwrap().deactivate(FailureType::new_failure_type(sendSnap.to_string(),"post".to_string()),str.clone());
            self.raft.report_unreachable(m.get_to());
            self.raft.report_snapshot(m.get_to(),raft::SnapshotStatus::Failure);
            return;
        }

        self.status.lock().unwrap().activate();
        self.raft.report_snapshot(m.get_to(),raft::SnapshotStatus::Finish);
        info!(default_logger(),"sent database snapshot snapshot-index =>{} remote-peer-id=>{} bytes=>{}  size =>{}",
        m.get_snapshot().get_metadata().get_index(),
        m.get_to(),
        snapshotSizeVal,
        snapshotSize.clone().as_str());
        return;
    }
    pub async fn post(&self, req:Request<Body>,check_req:Request<Body>) -> Result<(),Error> {
        let mut client = self.tr.lock().unwrap().pipeline_client.clone();
                // .expect("failed to get pipeline client")
                // .request(req)
                // .await;
        let resp = client.unwrap().request(req).await;


        let res = Channel::new(1);
        res.send(resp.unwrap()).await.expect("failed to send response");

        select! {
            _ = self.stopc.recv() =>{
                return Err(Error::new(std::io::ErrorKind::Other, "stopped"));
            }
            responce = res.recv() =>{
                return check_post_response(&mut responce.unwrap(),check_req, self.to).await;
            }
        }
        // return Err(Error::new(std::io::ErrorKind::Other, "failed to post snapshot"));

    }

}

pub fn create_snap_body(merged:SnapMessage) -> Vec<u8> {
    return protoMessage::write_to_bytes(&merged.get_msg()).expect("failed to write snapshot message to bytes");
}

#[cfg(test)]
mod tests{
    use std::sync::{Arc, Mutex};
    use openssl::x509::extension::ExtendedKeyUsage;
    use slog::info;
    use tracing::Instrument;
    use url::Url;
    use client::pkg::transport::listener::{new_tls_acceptor, self_cert, TLSInfo};
    use raft::default_logger;
    use raft::eraftpb::{Message, MessageType};
    use crate::etcdserver::api::rafthttp::peer_status::PeerStatus;
    use crate::etcdserver::api::rafthttp::snap::message::SnapMessage;
    use crate::etcdserver::api::rafthttp::snapshot_sender::new_snapshot_sender;
    use crate::etcdserver::api::rafthttp::test_util::{new_tr, server_err, server_succ};
    use crate::etcdserver::api::rafthttp::types::id::ID;
    use crate::etcdserver::api::rafthttp::types::urls::URLs;
    use crate::etcdserver::api::rafthttp::url_pick::urlPicker;
    use crate::etcdserver::async_ch::Channel;

    #[tokio::test]
    async fn test_snapshot_send(){
        let mut msg =  Message::default();
        msg.to=1;
        msg.msg_type = MessageType::MsgSnapshot;
        let sm = SnapMessage::new_snap_message(msg,Channel::new(1));
        test_snapshot_sender_succ(sm).await;
    }

    #[tokio::test]
    async fn test_snapshot_send_err(){
        let mut msg =  Message::default();
        msg.to=1;
        msg.msg_type = MessageType::MsgSnapshot;
        let sm = SnapMessage::new_snap_message(msg,Channel::new(1));
        test_snapshot_sender_err(sm).await;
    }

    async fn test_snapshot_sender_succ(sm:SnapMessage) {
        let urls = URLs::new(vec![Url::parse("https://localhost:2380").unwrap()]);
        let hosts = vec!["localhost"];
        let dirpath = "/tmp/test_self_cert";
        let self_signed_cert_validity = 365;
        let mut binding = ExtendedKeyUsage::new();
        let additional_usages = binding.client_auth();
        let info = self_cert(dirpath, hosts, self_signed_cert_validity, Some(additional_usages)).expect("failed to create self signed cert");
        let picker = urlPicker::new_url_picker(urls);
        let tr = new_tr("https://localhost:2380".to_string(),info.clone());
        server_succ(info.clone()).await;
        let mut snapsend = new_snapshot_sender(tr, picker, Arc::new(Mutex::new(PeerStatus::new(ID::new(0), ID::new(1)))), ID::new(1));
        info!(default_logger(),"start send");
        snapsend.send(sm).await;
        assert_eq!(snapsend.status.lock().unwrap().is_active(),true);
    }

    async fn test_snapshot_sender_err(sm:SnapMessage) {
        let urls = URLs::new(vec![Url::parse("https://localhost:2380").unwrap()]);
        let hosts = vec!["localhost"];
        let dirpath = "/tmp/test_self_cert";
        let self_signed_cert_validity = 365;
        let mut binding = ExtendedKeyUsage::new();
        let additional_usages = binding.client_auth();
        let info = self_cert(dirpath, hosts, self_signed_cert_validity, Some(additional_usages)).expect("failed to create self signed cert");
        let picker = urlPicker::new_url_picker(urls);
        let tr = new_tr("https://localhost:2380".to_string(),info.clone());
        server_err(info.clone()).await;
        let mut snapsend = new_snapshot_sender(tr, picker, Arc::new(Mutex::new(PeerStatus::new(ID::new(0), ID::new(1)))), ID::new(1));
        info!(default_logger(),"start send");
        snapsend.send(sm).await;
        assert_eq!(snapsend.status.lock().unwrap().is_active(),false);
    }
}


