use std::path::Path;
use lazy_static::lazy_static;
use slog::{debug, info, warn};
use raft::default_logger;
use raft::eraftpb::Message;
use crate::etcdserver::api::rafthttp::http::{errClusterIDMismatch, errIncompatibleVersion, RaftStreamPrefix};
use raft::eraftpb::MessageType;
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use hyper::{Body, Method, Request, StatusCode};
use hyper::body::{HttpBody, to_bytes};
use tokio::{select, time};
use crate::etcdserver::api::rafthttp::peer_status::PeerStatus;
use crate::etcdserver::api::rafthttp::transport::{Raft, Transport};
use crate::etcdserver::api::rafthttp::types::id::ID;
use crate::etcdserver::api::rafthttp::v2state::leader::FollowerStats;
use crate::etcdserver::async_ch::Channel;
use protobuf::{Message as protoMessage, ProtobufEnum};
use semver::Version;
use crate::etcdserver::api::rafthttp::url_pick::urlPicker;
use crate::etcdserver::api::rafthttp::util::net_util::{compare_major_minor_version, CustomError, server_version, set_peer_urls_header};
use crate::etcdserver::api::rafthttp::util::version;

const errUnsupportedStreamType:&str = "unsupported stream type";
const streamTypeMessage: &str = "message";
const streamTypeMsgAppV2: &str = "msgappv2";
const streamBufSize: usize = 4096;
const ConnReadTimeout: i32 = 5;
const ConnWriteTimeout: i32 = 5;

lazy_static!(

    static ref errMemberRemoved: Error = {return Error::new(ErrorKind::Other, CustomError::MemberRemoved);};

    static ref linkHeartbeatMessage: Message = {
        let mut msg = Message::default();
        msg.msg_type = MessageType::MsgHeartbeat;
        return msg;
    };
    static ref supportedStream: HashMap<String, Vec<&'static str>> = {
    let mut map = HashMap::new();
    map.insert("2.0.0".to_string(), vec![]);
    map.insert("2.1.0".to_string(), vec![streamTypeMsgAppV2, streamTypeMessage]);
    map.insert("2.2.0".to_string(), vec![streamTypeMsgAppV2, streamTypeMessage]);
    map.insert("2.3.0".to_string(), vec![streamTypeMsgAppV2, streamTypeMessage]);
    map.insert("3.0.0".to_string(), vec![streamTypeMsgAppV2, streamTypeMessage]);
    map.insert("3.1.0".to_string(), vec![streamTypeMsgAppV2, streamTypeMessage]);
    map.insert("3.2.0".to_string(), vec![streamTypeMsgAppV2, streamTypeMessage]);
    map.insert("3.3.0".to_string(), vec![streamTypeMsgAppV2, streamTypeMessage]);
    map.insert("3.4.0".to_string(), vec![streamTypeMsgAppV2, streamTypeMessage]);
    map.insert("3.5.0".to_string(), vec![streamTypeMsgAppV2, streamTypeMessage]);
    map.insert("3.6.0".to_string(), vec![streamTypeMsgAppV2, streamTypeMessage]);
    map
};
);

pub fn endpoint(t: &str) -> String {
    match t {
        streamTypeMsgAppV2 => return Path::new(RaftStreamPrefix).join("msgapp").to_str().unwrap().to_string(),
        streamTypeMessage => return Path::new(RaftStreamPrefix).join("message").to_str().unwrap().to_string(),
        _ => {
            info!(default_logger(),"unhandled stream type: {}", t);
            return "".to_string()
        }
        }
}

pub fn stream_type_to_string(t: &str) ->  String {
    match t {
        streamTypeMsgAppV2 => return "stream MsgApp v2".to_string(),
        streamTypeMessage => return "stream Message".to_string(),
        _ => return "unknown stream".to_string(),
    }
}

pub fn is_link_heartbeat_message(msg: Message) -> bool {
    return msg.msg_type == MessageType::MsgHeartbeat && msg.from == 0 && msg.to == 0;
}


pub struct outgoingConn{
    stream_type : String,
    local_id : ID,
    peer_id: ID,
}

pub struct stream{
    local_id : ID,
    peer_id: ID,

    status:Arc<Mutex<PeerStatus>>,
    fs : FollowerStats,
    raft: Arc<Box<dyn Raft + Send + Sync>>,

    working : bool,
    msgc :Channel<Message>,
    connc :Channel<outgoingConn>,
    stopc: Channel<()>,
    done : Channel<()>,
    stream_type : String,
    tr :Arc<Mutex<Transport>>,
    picker : urlPicker,
    recvc : Channel<Message>,
    propc : Channel<Message>,
    errorc : Channel<Error>,
    paused : bool,
}

impl stream{
    
    pub async fn run(&self){
        let  msgc = Channel::new(streamBufSize);
        let  heart_beats = Channel::new(1);

        let  (tickc_tx,mut tickc_rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move{
            loop{
                time::sleep(Duration::from_secs(ConnReadTimeout as u64)).await;
                tickc_tx.send(()).await.expect("failed to send tick signal");
            }
        });

        let unflushed = 0;
        info!(default_logger(),"started stream writer with remote peer";
            "local-member-id" => self.local_id.get(),
            "remote-peer-id" => self.peer_id.get());

        loop{
            select!{
                _ = tickc_rx.recv() =>{

                }
            }
        }
    }

    pub fn writec(&self) -> (Channel<Message>,bool) {
        return (self.msgc.clone(),self.working);
    }

    pub async fn stop(&self){
        self.stopc.send(()).await.expect("failed to send stop signal");
    }

    pub async fn decode(&self, data:Body,stream_type:String){
    let mut dec:Message = Message::default();
    match stream_type.as_str() {
        streamTypeMsgAppV2 =>{},
        streamTypeMessage =>{
           dec =  protoMessage::parse_from_bytes
                (to_bytes(data).await.expect("failed to read response body").as_ref()).
                expect("failed to parse message");
        },
        _ => {
            panic!("unknown stream type {}",stream_type);
        }
    }

    // if self.paused{
    //     return;
    // }

    if is_link_heartbeat_message(dec.clone()){
        return;
    }

    let mut recvc =&self.recvc;
    if dec.msg_type == MessageType::MsgPropose{
        recvc = &self.propc;
    }


    match recvc.send(dec.clone()).await{
        Ok(_)=>{},
        Err(e)=>{
            if self.status.lock().unwrap().is_active(){
                warn!(default_logger(),"dropped internal Raft message since receiving buffer is full (overloaded network) remote-peer-active => true";
                    "local-member-id" => self.tr.lock().unwrap().get_id().get(),
                    "remote-peer-id" => self.peer_id.get(),
                    "error" => e.to_string(),
                    "message-type" => ProtobufEnum::value(&dec.msg_type),
                    "from" => dec.from);
            }
            else {
                warn!(default_logger(),"dropped Raft message since receiving buffer is full (overloaded network) remote-peer-active => false";
                    "local-member-id" => self.tr.lock().unwrap().get_id().get(),
                    "remote-peer-id" => self.peer_id.get(),
                    "error" => e.to_string(),
                    "message-type" => ProtobufEnum::value(&dec.msg_type),
                    "from" => dec.from);
            }
        }
    }
}

    pub async fn dial(&self,stream_type:String) -> Result<Body,Error>{
        let u = self.picker.get_base_url_picker().try_lock().unwrap().pick().expect("failed to pick url");
        let mut uu =u.clone();
        uu.set_path(endpoint(&stream_type).as_str());
        uu.set_path(self.tr.lock().unwrap().ID.to_string().as_str());

        debug!(default_logger(),"dial stream reader from=>{} to=>{} address=>{}",
        self.tr.lock().unwrap().ID.to_string(),
        self.peer_id.to_string(),
        uu.to_string());

        let mut req = Request::builder()
            .method(Method::GET)
            .header("X-Server-From",self.tr.lock().unwrap().get_id().to_string())
            .header("X-Server-Version",version::Version)
            .header("X-Min-Cluster-Version",version::MinClusterVersion)
            .header("X-Etcd-Cluster-ID",self.tr.lock().unwrap().get_cluster_id().to_string())
            .header("X-Raft-To",self.peer_id.to_string())
            .uri(uu.to_string());
        let request =  set_peer_urls_header(req,self.tr.lock().unwrap().get_urls());
        // request.body(Body::empty()).expect("failed to build request body");
        let resp =  self.tr.lock().unwrap().stream_client.clone().unwrap()
            .request(request.body(Body::empty()).expect("failed to build request body"))
            .await;

        if resp.is_err(){
            self.picker.get_base_url_picker().lock().unwrap().unreachable(u.clone());
            return Err(Error::new(ErrorKind::Other, resp.err().unwrap().to_string()));;
        }

        let peerID=self.peer_id.to_string();
        let id = self.tr.lock().unwrap().get_id().to_string();

        let response = resp.unwrap();
        let rv = server_version(response.headers());
        let lv = Version::from_str(version::Version);
        if compare_major_minor_version(&rv.unwrap(),&lv.unwrap()) == -1{
            self.picker.get_base_url_picker().lock().unwrap().unreachable(u.clone());
            return Err(Error::new(ErrorKind::Other, errUnsupportedStreamType.to_string()));
        }
        match response.status() {
            StatusCode::GONE => {
                self.errorc.send(Error::new(ErrorKind::Other, CustomError::MemberRemoved)).await.expect("failed to send error");
                return Err(Error::new(ErrorKind::Other, CustomError::MemberRemoved));
            }

            StatusCode::OK => {
                return Ok(response.into_body());
            }

            StatusCode::NOT_FOUND => {
                self.picker.get_base_url_picker().lock().unwrap().unreachable(u.clone());
                return Err(Error::new(ErrorKind::Other, format!("peer {} failed to find local node {}",peerID,id)));
            }

            StatusCode::PRECONDITION_FAILED =>{
                self.picker.get_base_url_picker().lock().unwrap().unreachable(u.clone());
                let rep_byte =  to_bytes(response.into_body()).await.expect("failed to read response body");
                let rep = String::from_utf8(rep_byte.to_vec()).expect("failed to convert response body to string");

                if rep.contains(&errIncompatibleVersion.to_string()){
                    warn!(default_logger(),"request sent was ignored by remote peer due to server version incompatibility local-member-id =>{} remote-peer-id=>{} {}",
                            id,
                            peerID,
                            errIncompatibleVersion.to_string());
                    return Err(Error::new(ErrorKind::Other, errIncompatibleVersion.to_string()));
                }
                else if rep.contains(&errClusterIDMismatch.to_string()) {
                    warn!(default_logger(),"request sent was ignored by remote peer due to cluster ID mismatch local-member-id =>{} remote-peer-id=>{} {}",
                            id,
                            peerID,
                            errClusterIDMismatch.to_string());
                    return Err(Error::new(ErrorKind::Other, errClusterIDMismatch.to_string()));
                }
                else{
                    return Err(Error::new(ErrorKind::Other, format!("unhandled error {} when precondition failed",rep)));
                };
            }

            code =>{
                self.picker.get_base_url_picker().lock().unwrap().unreachable(u.clone());
                return Err(Error::new(ErrorKind::Other, format!("unhandled http status {}",code.to_string())));
            }
        }
    }
}

// impl streamWriter{
//
//     pub async fn run(&self){
//         // let  msgc = Channel::new(streamBufSize);
//         // let  heart_beats = Channel::new(10);
//
//         let  (tickc_tx,mut tickc_rx) = tokio::sync::mpsc::channel(1);
//         tokio::spawn(async move{
//             loop{
//                 time::sleep(Duration::from_secs(ConnReadTimeout as u64)).await;
//                 tickc_tx.send(()).await.expect("failed to send tick signal");
//             }
//         });
//
//         let unflushed = 0;
//         info!(default_logger(),"started stream writer with remote peer";
//             "local-member-id" => self.local_id.get(),
//             "remote-peer-id" => self.peer_id.get());
//
//         loop{
//             select!{
//                 _ = tickc_rx.recv() =>{
//
//                 }
//             }
//         }
//     }
//
//
//     pub fn writec(&self) -> (Channel<Message>,bool) {
//         return (self.msgc.clone(),self.working);
//     }
//
//     pub fn close(&mut self) -> bool {
//        return  self.close_unlocked();
//     }
//
//     pub fn close_unlocked(&mut self) -> bool{
//         if !self.working{
//             return false;
//         };
//         if self.msgc.is_empty(){
//             self.raft.report_unreachable(self.peer_id.get());
//         };
//
//         self.msgc = Channel::new(streamBufSize);
//         self.working = false;
//         return true;
//     }
//
//     pub async fn attach(&self, conn:outgoingConn) -> bool{
//         select!(
//             _= self.connc.send(conn)=>{true},
//             _= self.done.recv() => {false},
//         )
//     }
//
//     pub async fn stop(&self){
//         self.stopc.send(()).await.expect("failed to send stop signal");
//     }
// }
//
// pub struct streamReader{
//     peer_id: ID,
//
//     stream_type : String,
//     tr :Arc<Mutex<Transport>>,
//     picker : urlPicker,
//     status:Arc<Mutex<PeerStatus>>,
//     recvc : Channel<Message>,
//     propc : Channel<Message>,
//     errorc : Channel<Error>,
//     paused : bool,
//     done : Channel<()>,
// }
//
// impl streamReader{
//
//     pub async fn run(&self){
//
//     }
//
//     pub async fn decodeLoop(&self, data:Body,stream_type:String){
//         let mut dec:Message = Message::default();
//         match stream_type.as_str() {
//             streamTypeMsgAppV2 =>{},
//             streamTypeMessage =>{
//                dec =  protoMessage::parse_from_bytes
//                     (to_bytes(data).await.expect("failed to read response body").as_ref()).
//                     expect("failed to parse message");
//             },
//             _ => {
//                 panic!("unknown stream type {}",stream_type);
//                 // return;
//             }
//         }
//
//         if self.paused{
//             return;
//         }
//
//         if is_link_heartbeat_message(dec.clone()){
//             return;
//         }
//
//         let mut recvc =&self.recvc;
//         if dec.msg_type == MessageType::MsgPropose{
//             recvc = &self.propc;
//         }
//
//
//         match recvc.send(dec.clone()).await{
//             Ok(_)=>{},
//             Err(e)=>{
//                 if self.status.lock().unwrap().is_active(){
//                     warn!(default_logger(),"dropped internal Raft message since receiving buffer is full (overloaded network) remote-peer-active => true";
//                         "local-member-id" => self.tr.lock().unwrap().get_id().get(),
//                         "remote-peer-id" => self.peer_id.get(),
//                         "error" => e.to_string(),
//                         "message-type" => ProtobufEnum::value(&dec.msg_type),
//                         "from" => dec.from);
//                 }
//                 else {
//                     warn!(default_logger(),"dropped Raft message since receiving buffer is full (overloaded network) remote-peer-active => false";
//                         "local-member-id" => self.tr.lock().unwrap().get_id().get(),
//                         "remote-peer-id" => self.peer_id.get(),
//                         "error" => e.to_string(),
//                         "message-type" => ProtobufEnum::value(&dec.msg_type),
//                         "from" => dec.from);
//                 }
//             }
//         }
//     }
//
//     pub async fn stop(&self){
//         self.done.send(()).await.expect("failed to send done signal");
//     }
//
//     pub async fn dial(&self,stream_type:String) -> Result<Body,Error>{
//         let u = self.picker.get_base_url_picker().try_lock().unwrap().pick().expect("failed to pick url");
//         let mut uu =u.clone();
//         uu.set_path(endpoint(&stream_type).as_str());
//         uu.set_path(self.tr.lock().unwrap().ID.to_string().as_str());
//
//         debug!(default_logger(),"dial stream reader from=>{} to=>{} address=>{}",
//         self.tr.lock().unwrap().ID.to_string(),
//         self.peer_id.to_string(),
//         uu.to_string());
//
//         let mut req = Request::builder()
//             .method(Method::GET)
//             .header("X-Server-From",self.tr.lock().unwrap().get_id().to_string())
//             .header("X-Server-Version",version::Version)
//             .header("X-Min-Cluster-Version",version::MinClusterVersion)
//             .header("X-Etcd-Cluster-ID",self.tr.lock().unwrap().get_cluster_id().to_string())
//             .header("X-Raft-To",self.peer_id.to_string())
//             .uri(uu.to_string());
//         let request =  set_peer_urls_header(req,self.tr.lock().unwrap().get_urls());
//         // request.body(Body::empty()).expect("failed to build request body");
//         let resp =  self.tr.lock().unwrap().stream_client.clone().unwrap()
//             .request(request.body(Body::empty()).expect("failed to build request body"))
//             .await;
//
//         if resp.is_err(){
//             self.picker.get_base_url_picker().lock().unwrap().unreachable(u.clone());
//             return Err(Error::new(ErrorKind::Other, resp.err().unwrap().to_string()));;
//         }
//
//         let peerID=self.peer_id.to_string();
//         let id = self.tr.lock().unwrap().get_id().to_string();
//
//         let response = resp.unwrap();
//         let rv = server_version(response.headers());
//         let lv = Version::from_str(version::Version);
//         if compare_major_minor_version(&rv.unwrap(),&lv.unwrap()) == -1{
//             self.picker.get_base_url_picker().lock().unwrap().unreachable(u.clone());
//             return Err(Error::new(ErrorKind::Other, errUnsupportedStreamType.to_string()));
//         }
//         match response.status() {
//             StatusCode::GONE => {
//                 self.errorc.send(Error::new(ErrorKind::Other, CustomError::MemberRemoved)).await.expect("failed to send error");
//                 return Err(Error::new(ErrorKind::Other, CustomError::MemberRemoved));
//             }
//
//             StatusCode::OK => {
//
//                 return Ok(response.into_body());
//             }
//
//             StatusCode::NOT_FOUND => {
//                 self.picker.get_base_url_picker().lock().unwrap().unreachable(u.clone());
//                 return Err(Error::new(ErrorKind::Other, format!("peer {} failed to find local node {}",peerID,id)));
//             }
//
//             StatusCode::PRECONDITION_FAILED =>{
//                 self.picker.get_base_url_picker().lock().unwrap().unreachable(u.clone());
//                 let rep_byte =  to_bytes(response.into_body()).await.expect("failed to read response body");
//                 let rep = String::from_utf8(rep_byte.to_vec()).expect("failed to convert response body to string");
//
//                 if rep.contains(&errIncompatibleVersion.to_string()){
//                     warn!(default_logger(),"request sent was ignored by remote peer due to server version incompatibility local-member-id =>{} remote-peer-id=>{} {}",
//                             id,
//                             peerID,
//                             errIncompatibleVersion.to_string());
//                     return Err(Error::new(ErrorKind::Other, errIncompatibleVersion.to_string()));
//                 }
//                 else if rep.contains(&errClusterIDMismatch.to_string()) {
//                     warn!(default_logger(),"request sent was ignored by remote peer due to cluster ID mismatch local-member-id =>{} remote-peer-id=>{} {}",
//                             id,
//                             peerID,
//                             errClusterIDMismatch.to_string());
//                     return Err(Error::new(ErrorKind::Other, errClusterIDMismatch.to_string()));
//                 }
//                 else{
//                     return Err(Error::new(ErrorKind::Other, format!("unhandled error {} when precondition failed",rep)));
//                 };
//             }
//
//             code =>{
//                 self.picker.get_base_url_picker().lock().unwrap().unreachable(u.clone());
//                 return Err(Error::new(ErrorKind::Other, format!("unhandled http status {}",code.to_string())));
//             }
//         }
//     }
//
//     pub fn pause(&mut self){
//         self.paused = true;
//     }
//
//     pub fn resume(&mut self){
//         self.paused = false;
//     }
// }
//
// pub fn check_stream_support(v :Version, stream_type : &str) -> bool{
//     let mut ok = false;
//     let version =  Version::new(v.major,v.minor,0);
//     if let Some(supported) = supportedStream.get(&version.to_string()){
//         for t in supported{
//             if t == &stream_type{
//                 ok = true;
//                 break;
//             }
//         }
//     }
//     return ok;
// }