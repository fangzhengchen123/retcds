use futures::SinkExt;
use raft::eraftpb::Message;
use crate::etcdserver::api::rafthttp::error::Error;
use crate::etcdserver::async_ch::Channel;

#[derive(Clone)]
pub struct SnapMessage{
    msg: Message,
    // read_closer: ExactReaderCloser,
    total_size: i64,
    close_c : Channel<bool>,
}

impl SnapMessage{
    pub fn new_snap_message(msg: Message, close_c : Channel<bool>) -> Self {
        SnapMessage{
            msg: msg.clone(),
            // read_closer: ExactReaderCloser::new(read_closer,total_size ),
            total_size: protobuf::Message::write_to_bytes(&msg.clone()).unwrap().len() as i64,
            close_c : close_c,
        }
    }

    pub fn close_notify(&self) -> Channel<bool> {
        self.close_c.clone()
    }

    pub async fn close_with_error(&self) -> Result<(), Error> {
        let result = self.close_c.send(true).await;
        if result.is_err() {
                    self.close_c.send(false).await;
                    return Err(Error::ErrSend)
                };
        Ok(())
    }

    pub fn get_msg(&self) -> Message {
        self.msg.clone()
    }

    pub fn get_total_size(&self) -> i64 {
        self.total_size.clone()
    }
}