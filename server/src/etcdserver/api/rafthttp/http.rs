use std::string::ToString;

pub const RaftPrefix:&str = "/raft";
pub const RaftSnapshotPrefix:&str ="/raft/snapshot";
pub const RaftStreamPrefix:&str = "/raft/stream";

pub const  errIncompatibleVersion:&str ="incompatible version";
pub const  errClusterIDMismatch:&str = "cluster ID mismatch";
// const ProbingPrefix