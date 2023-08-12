use std::path::Path;
use lazy_static::lazy_static;
use slog::{debug, info, warn};
use crate::etcdserver::api::rafthttp::util::default_logger;
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
use crate::etcdserver::api::rafthttp::peer_status::{FailureType, PeerStatus};
use crate::etcdserver::api::rafthttp::transport::{Raft, Transport};
use crate::etcdserver::api::rafthttp::types::id::ID;
use crate::etcdserver::async_ch::Channel;
use protobuf::{Message as protoMessage, ProtobufEnum};
use semver::Version;
use  slog::error;
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
    raft: Arc<Box<dyn Raft + Send + Sync>>,

    working : bool,
    msgc :Channel<Message>,
    stopc: Channel<()>,
    stream_type : String,
    tr :Transport,
    picker : urlPicker,
    recvc : Channel<Message>,
    propc : Channel<Message>,
    errorc : Channel<Error>,
}

impl stream{

    pub async fn start(tr: Transport,
                            picker: urlPicker,
                            status: PeerStatus,
                            local:ID,
                            id:ID,
                            raft: Box<dyn Raft + Send + Sync>){
        let mut st = stream {
            local_id: local,
            peer_id: id,
            status: Arc::new(Mutex::new(status)),
            raft: Arc::new(raft),
            working: true,
            msgc: Channel::new(streamBufSize),
            stopc: Channel::new(1),
            stream_type: "".to_string(),
            tr,
            picker,
            recvc: Channel::new(streamBufSize),
            propc: Channel::new(streamBufSize),
            errorc: Channel::new(10),
        };
        // tokio::spawn(async move{
        //     st.run().await;
        // });

        st.run().await;

    }
    
    pub async fn run(&mut self){
        let  (tickc_tx,mut tickc_rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move{
            loop{
                time::sleep(Duration::from_secs(ConnReadTimeout as u64)).await;
                tickc_tx.send(()).await.expect("failed to send tick signal");
            }
        });

        info!(default_logger(),"started stream writer with remote peer";
            "local-member-id" => self.local_id.get(),
            "remote-peer-id" => self.peer_id.get());

        loop{
            select!{
                _ = tickc_rx.recv() =>{
                    let resp = self.dial(streamTypeMessage.to_string(),None).await;

                    if resp.is_err(){
                        self.status.lock().unwrap().deactivate(FailureType::new_failure_type(streamTypeMessage.to_string(), "heartbeat".to_string()),"responce error".to_string());
                        self.close();
                        error!(default_logger(),"failed to receive message from remote peer";
                            "local-member-id" => self.local_id.get(),
                            "remote-peer-id" => self.peer_id.get());
                        return;
                    }
                    let decode =  self.decode(resp.unwrap(),streamTypeMessage.to_string()).await;
                    if decode.is_ok(){
                        continue;
                    }
                    let decode_err =  decode.err().unwrap().to_string();
                    self.status.lock().unwrap().deactivate(FailureType::new_failure_type(streamTypeMessage.to_string(), "heartbeat".to_string()),decode_err.clone());
                    self.close();
                    error!(default_logger(),"failed to receive message from remote peer";
                        "local-member-id" => self.local_id.get(),
                        "remote-peer-id" => self.peer_id.get(),
                        "error" => decode_err.clone());
                    return;
                }
                msg = self.msgc.recv() =>{
                    let mut msg = msg.unwrap();
                    let resp = self.dial(streamTypeMessage.to_string(),Some(msg.clone())).await;
                    let decode =  self.decode(resp.unwrap(),streamTypeMessage.to_string()).await;
                    if decode.is_ok(){
                        continue;
                    }
                    let decode_err =  decode.err().unwrap().to_string();
                    self.status.lock().unwrap().deactivate(FailureType::new_failure_type(streamTypeMessage.to_string(), "heartbeat".to_string()),decode_err.clone());
                    self.close();
                    error!(default_logger(),"failed to receive message from remote peer";
                        "local-member-id" => self.local_id.get(),
                        "remote-peer-id" => self.peer_id.get(),
                        "error" => decode_err.clone());
                    return;
                }

            }
        }
    }

    pub fn writec(&self) -> (&Channel<Message>,bool) {
        return (&self.msgc,self.working);
    }

    pub async fn stop(&self){
        self.stopc.send(()).await.expect("failed to send stop signal");
    }

    pub async fn decode(&self, data:Body,stream_type:String) -> Result<(),Error>{
    let mut dec:Message = Message::default();
    // warn!(default_logger(),"{}",stream_type);
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



    if is_link_heartbeat_message(dec.clone()){
        return Ok(());
    }

    let mut recvc =&self.recvc;
    if dec.msg_type == MessageType::MsgPropose{
        recvc = &self.propc;
    }

    match recvc.send(dec.clone()).await{
        Ok(_)=>{return Ok(())},
        Err(e)=>{
            if self.status.lock().unwrap().is_active(){
                warn!(default_logger(),"dropped internal Raft message since receiving buffer is full (overloaded network) remote-peer-active => true";
                    "local-member-id" => self.tr.get_id().get(),
                    "remote-peer-id" => self.peer_id.get(),
                    "error" => e.to_string(),
                    "message-type" => ProtobufEnum::value(&dec.msg_type),
                    "from" => dec.from);

            }
            else {
                warn!(default_logger(),"dropped Raft message since receiving buffer is full (overloaded network) remote-peer-active => false";
                    "local-member-id" => self.tr.get_id().get(),
                    "remote-peer-id" => self.peer_id.get(),
                    "error" => e.to_string(),
                    "message-type" => ProtobufEnum::value(&dec.msg_type),
                    "from" => dec.from);
            }
            return Err(Error::new(ErrorKind::Other, e.to_string()));
        }
    }
}

    pub async fn dial(&self,stream_type:String,msg:Option<Message>) -> Result<Body,Error>{
        let u = self.picker.get_base_url_picker().try_lock().unwrap().pick().expect("failed to pick url");
        let mut uu =u.clone();
        uu.set_path(format!("{}/{}",endpoint(&stream_type).as_str(),self.tr.ID.to_string().as_str()).as_str());

        debug!(default_logger(),"dial stream reader from=>{} to=>{} address=>{}",
        self.tr.ID.to_string(),
        self.peer_id.to_string(),
        uu.to_string());

        let mut req = Request::builder()
            .method(Method::GET)
            .header("X-Server-From",self.tr.get_id().to_string())
            .header("X-Server-Version",version::Version)
            .header("X-Min-Cluster-Version",version::MinClusterVersion)
            .header("X-Etcd-Cluster-ID",self.tr.get_cluster_id().to_string())
            .header("X-Raft-To",self.peer_id.to_string())
            .uri(uu.to_string());
        let request =  set_peer_urls_header(req,self.tr.get_urls());
        // request.body(Body::empty()).expect("failed to build request body");
        let mut body = Body::empty();
        if msg.is_some() {
            body = Body::from(protoMessage::write_to_bytes(&msg.unwrap()).expect("failed to write message to bytes"));
        };

        let resp =  self.tr.stream_client.clone().unwrap()
            .request(request.body(body).expect("failed to build request body"))
            .await;

        if resp.is_err(){
            self.picker.get_base_url_picker().lock().unwrap().unreachable(u.clone());
            return Err(Error::new(ErrorKind::Other, resp.err().unwrap().to_string()));;
        }

        let peerID=self.peer_id.to_string();
        let id = self.tr.get_id().to_string();

        let response = resp.unwrap();
        let rv = server_version(response.headers()).unwrap();
        let lv = Version::from_str(version::Version).unwrap();
        // warn!(default_logger(),"{}",compare_major_minor_version(&rv.unwrap().clone(),&lv.unwrap().clone()));
        if compare_major_minor_version(&rv.clone(),&lv.clone()) == -1 && !check_stream_support(rv.clone(),&stream_type){
            self.picker.get_base_url_picker().lock().unwrap().unreachable(u.clone());
            return Err(Error::new(ErrorKind::Other, errUnsupportedStreamType.to_string()));
        }
        match response.status() {
            StatusCode::GONE => {
                self.picker.get_base_url_picker().lock().unwrap().unreachable(u.clone());
                self.errorc.send(Error::new(ErrorKind::Other, CustomError::MemberRemoved)).await.expect("failed to send error");
                warn!(default_logger(),"{}",CustomError::MemberRemoved.to_string());
                return Err(Error::new(ErrorKind::Other, CustomError::MemberRemoved));
            }

            StatusCode::OK => {
                return Ok(response.into_body());
            }

            StatusCode::NOT_FOUND => {
                self.picker.get_base_url_picker().lock().unwrap().unreachable(u.clone());
                let err_str =  format!("peer {} failed to find local node {}",peerID,id);
                warn!(default_logger(),"{}",err_str.clone());
                return Err(Error::new(ErrorKind::Other, err_str.clone()));
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

    pub fn close(&mut self) -> bool {
       return  self.close_unlocked();
    }

    pub fn close_unlocked(&mut self) -> bool{
        if !self.working{
            return false;
        };
        if self.msgc.is_empty(){
            self.raft.report_unreachable(self.peer_id.get());
        };

        self.msgc = Channel::new(streamBufSize);
        self.working = false;
        return true;
    }
}

pub fn check_stream_support(v :Version, stream_type : &str) -> bool{
    let mut ok = false;
    let version =  Version::new(v.major,v.minor,0);
    if let Some(supported) = supportedStream.get(&version.to_string()){
        for t in supported{
            if t == &stream_type{
                ok = true;
                break;
            }
        }
    }
    return ok;
}

#[cfg(test)]
mod tests{
    use std::thread;
    use std::time::Duration;
    use openssl::x509::extension::ExtendedKeyUsage;
    use url::Url;
    use client::pkg::transport::listener::self_cert;
    use crate::etcdserver::api::rafthttp::peer_status::PeerStatus;
    use crate::etcdserver::api::rafthttp::stream::stream;
    use crate::etcdserver::api::rafthttp::test_util::{fakeRaft, new_tr, server_err, server_succ};
    use crate::etcdserver::api::rafthttp::types::id::ID;
    use crate::etcdserver::api::rafthttp::types::urls::URLs;
    use crate::etcdserver::api::rafthttp::url_pick::urlPicker;
    use crate::etcdserver::async_ch::Channel;

    #[tokio::test]
    async fn tests_send(){
        let urls = URLs::new(vec![Url::parse("https://localhost:2380").unwrap()]);
        let hosts = vec!["localhost"];
        let dirpath = "/tmp/test_self_cert";
        let self_signed_cert_validity = 365;
        let mut binding = ExtendedKeyUsage::new();
        let additional_usages = binding.client_auth();
        let info = self_cert(dirpath, hosts, self_signed_cert_validity, Some(additional_usages)).unwrap();
        let picker = urlPicker::new_url_picker(urls);
        let tr = new_tr("https://localhost:2380".to_string(),info.clone());
        server_succ(info.clone()).await;

        stream::start(tr,picker,PeerStatus::new(ID::new(0),ID::new(1)),ID::new(0),ID::new(1),Box::new(fakeRaft {
            recvc: Channel::new(1),
            err: "error".to_string(),
            removed_id: 1,
        })).await;

        // thread::sleep(Duration::from_secs(5 * 60));
    }


}