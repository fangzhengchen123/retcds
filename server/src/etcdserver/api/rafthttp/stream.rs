use std::path::Path;
use lazy_static::lazy_static;
use slog::info;
use raft::default_logger;
use raft::eraftpb::Message;
use crate::etcdserver::api::rafthttp::http::RaftStreamPrefix;
use raft::eraftpb::MessageType;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::{select, time};
use crate::etcdserver::api::rafthttp::peer_status::PeerStatus;
use crate::etcdserver::api::rafthttp::transport::Raft;
use crate::etcdserver::api::rafthttp::types::id::ID;
use crate::etcdserver::api::rafthttp::v2state::leader::FollowerStats;
use crate::etcdserver::async_ch::Channel;
use protobuf::Message as protoMessage;

const errUnsupportedStreamType:&str = "unsupported stream type";
const streamTypeMessage: &str = "message";
const streamTypeMsgAppV2: &str = "msgappv2";
const streamBufSize: usize = 4096;
const ConnReadTimeout: i32 = 5;
const ConnWriteTimeout: i32 = 5;

lazy_static!(
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

pub struct streamWriter{
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
}

impl streamWriter{

    pub async fn run(&self){
        // let  msgc = Channel::new(streamBufSize);
        // let  heart_beats = Channel::new(10);

        let  (tickc_tx,tickc_rx) = tokio::sync::mpsc::channel(1);
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

    pub async fn attach(&self, conn:outgoingConn) -> bool{
        select!(
            _= self.connc.send(conn)=>{true},
            _= self.done.recv() => {false},
        )
    }

    pub async fn stop(&self){
        self.stopc.send(()).await.expect("failed to send stop signal");
    }
}
