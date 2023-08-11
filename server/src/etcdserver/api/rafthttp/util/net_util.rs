use std::cmp::Ordering;
use std::error::Error;
use std::fmt;
use std::fmt::Debug;
use std::io::ErrorKind;
use hyper::{Body, Method, Request, Response, StatusCode};
use hyper::body::HttpBody;
use hyper::http::HeaderValue;
use hyper::http::request::Builder;
use semver::{BuildMetadata, Prerelease, Version};
use slog::{error, info, warn};
// use openssl::version::version;
use url::Url;
use crate::etcdserver::api::rafthttp::types::id::ID;
use crate::etcdserver::api::rafthttp::types::urls::URLs;
use crate::etcdserver::api::rafthttp::util::version;
use crate::etcdserver::api::rafthttp::util::default_logger;
#[derive(Debug)]
pub enum CustomError {
    IncompatibleVersion,
    ClusterIDMismatch,
    MemberRemoved,
    Other(String),
}

impl Error for CustomError {}

impl fmt::Display for CustomError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CustomError::IncompatibleVersion => write!(f, "incompatible version"),
            CustomError::ClusterIDMismatch => write!(f, "Cluster ID mismatch"),
            CustomError::MemberRemoved => write!(f, "the member has been permanently removed from the cluster"),
            CustomError::Other(msg) => write!(f, "Unhandled error: {}", msg),
        }
    }
}

pub fn create_POST_request(u:Url, path:&str, body:Vec<u8>, ct:&str, urls:URLs, from:ID, cid:ID) -> hyper::http::Result<Request<Body>> {
    let mut uu = u;
    uu.set_path(path);
    let mut req = Request::builder()
        .method(Method::POST)
        .uri(uu.as_str())
        .header("Content-Type",ct)
        .header("X-Server-From",from.to_string())
        .header("X-Server-Version",version::Version)
        .header("X-Min-Cluster-Version",version::MinClusterVersion)
        .header("X-Etcd-Cluster-ID",cid.to_string());
    let request = set_peer_urls_header(req, urls);
    request.body(Body::from(body))


}

pub async fn check_post_response(resp: &mut Response<Body>, req:Request<Body>, to:ID) -> Result<(),std::io::Error>{
    match resp.status(){
        StatusCode::PRECONDITION_FAILED => {
            let body_str = String::from_utf8_lossy(&resp.data().await.unwrap().unwrap().to_vec()).trim_end().to_string();
            match body_str.as_str() {
                err if err.contains(&CustomError::IncompatibleVersion.to_string()) =>{
                    error!(default_logger(),"request sent was ignored by peer remote-peer-id={}",to.to_string());
                    Err(std::io::Error::new(ErrorKind::Other, err.to_string()))
                }

                err if err.contains(&CustomError::ClusterIDMismatch.to_string()) =>{
                    error!(default_logger(),"request sent was ignored due to cluster ID mismatch remote-peer-id =>{} remote-peer-cluster-id=>{} local-member-cluster-id=>{}",to.to_string()
                        ,resp.headers().get("X-Etcd-Cluster-ID").expect("error get X-Etcd-Cluster-ID(response))").to_str().unwrap()
                        ,req.headers().get("X-Etcd-Cluster-ID").expect("error get X-Etcd-Cluster-ID(request)").to_str().unwrap());
                    Err(std::io::Error::new(ErrorKind::Other, err.to_string()))
                }
                &_ => {
                    Err(std::io::Error::new(ErrorKind::Other, "".to_string()))
                    }
            }
        }
        StatusCode::FORBIDDEN =>{
            Err(std::io::Error::new(ErrorKind::Other, CustomError::MemberRemoved.to_string()))
        }
        StatusCode::NO_CONTENT =>{
            Ok(())
        }
        _ => {
            // format!("https status {} while posting to {}",resp.status().to_string(),req.uri().to_string());
            Err(std::io::Error::new(ErrorKind::Other, format!("unexpected https status {} while posting to {}",resp.status().to_string(),req.uri().to_string())))
        }
    }
}

pub fn compare_major_minor_version(a: &Version, b: &Version) -> i32 {
    let na = Version {
        major: a.major,
        minor: a.minor,
        patch: 0,
        pre: Default::default(),
        build: Default::default(),
    };

    let nb = Version {
        major: b.major,
        minor: b.minor,
        patch: 0,
        pre: Default::default(),
        build: Default::default(),
    };

    match na.cmp(&nb) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Greater => 1,
        _ => 0,
    }
}

pub fn server_version(headers: &hyper::HeaderMap<HeaderValue>) -> Result<Version, semver::Error> {
    if let Some(ver_str) = headers.get("X-Server-Version").and_then(|value| value.to_str().ok()) {
        Version::parse(ver_str)
    } else {
        Version::parse("2.0.0")
    }
}

pub fn min_cluster_version(headers: &hyper::HeaderMap<HeaderValue>) -> Result<Version, semver::Error> {
    if let Some(ver_str) = headers.get("X-Min-Cluster-Version").and_then(|value| value.to_str().ok()) {
        Version::parse(ver_str)
    } else {
        Version::parse("2.0.0")
    }
}


pub fn check_version_compatibility(
    name: &str,
    server: &Version,
    min_cluster: &Version,
    ) -> Result<(Version, Version), std::io::Error> {
    let local_server = Version::parse(version::Version).unwrap();
    let local_min_cluster = Version::parse(version::MinClusterVersion).unwrap();

    if compare_major_minor_version(server, &local_min_cluster) == -1 {
        return  Err(std::io::Error::new(ErrorKind::Other, format!(
            "remote version is too low: remote[{}]={}, local={}",
            name, server, local_server)));
    }
    if compare_major_minor_version(min_cluster, &local_min_cluster) == 1 {
        return  Err(std::io::Error::new(ErrorKind::Other,  format!(
            "local version is too low: remote[{}]={}, local={}",
            name, server, local_server
        )));
    }
    Ok((local_server, local_min_cluster))

}

pub fn set_peer_urls_header(req:Builder, urls:URLs) -> Builder{
    if urls.len() == 0{
        return req;
    }
    let mut peer_URLs = String::new();
    // let mut peer_urls = String::new();
    for u in urls.get_urls(){
        peer_URLs.push_str(u.as_str());
        peer_URLs.push_str(",");
    }

    req.header("X-PeerURLs",peer_URLs)

}

#[cfg(test)]
mod tests{
    use std::io;
    use hyper::HeaderMap;
    use hyper::http::HeaderValue;
    use raft::eraftpb::Entry;
    use protobuf::Message;
    use semver::{Version, VersionReq};
    use slog::warn;
    use crate::etcdserver::api::rafthttp::util::net_util::{check_version_compatibility, compare_major_minor_version, min_cluster_version, server_version};
    use crate::etcdserver::api::rafthttp::default_logger;
    use crate::etcdserver::api::rafthttp::util::version;

    #[test]
    fn test_entry(){
        let mut entry = Entry::new();
        entry.set_term(1);
        entry.set_index(1);
        let mut entry1 = Entry::new();
        entry1.set_term(1);
        entry1.set_index(1);

        entry1.set_data(bytes::Bytes::from("some data"));
        let entry2 = Entry::new();
        let tests = vec![entry,entry1,entry2];
        for test in tests{
            let entry_vec = write_entry_to(&test).unwrap();
            let ent = read_entry_from(entry_vec).unwrap();
            assert_eq!(test, ent);
        }
        // entry.(1);
    }

    #[test]
    fn test_compare_major_minor_version(){
        let tests = vec![
          test {
            va: Version::parse("2.1.0").unwrap(),
            vb: Version::parse("2.1.0").unwrap(),
            want: 0,
        },
          test {
            va: Version::parse("2.0.0").unwrap(),
            vb: Version::parse("2.1.1").unwrap(),
            want: -1,
        },
          test {
            va: Version::parse("2.2.0").unwrap(),
            vb: Version::parse("2.1.0").unwrap(),
            want: 1,
        },
            test{
                va: Version::parse("2.1.1").unwrap(),
                vb: Version::parse("2.1.0").unwrap(),
                want: 0,
            },
            test{
                va: Version::parse("2.1.0-alpha.0").unwrap(),
                vb: Version::parse("2.1.0").unwrap(),
                want: 0,
            },
        ];
        let mut i =0;
        for test in tests {
            warn!(default_logger(),"{}",i);
            i +=1;
            assert_eq!(compare_major_minor_version(&test.va, &test.vb),test.want);
            // compare_major_minor_version(&test.va, &test.vb) == test.want;
        }
    }

    #[test]
    fn test_server_version(){
        let mut h= HeaderMap::new();
        h.insert("X-Server-Version",HeaderValue::from_static("2.1.0"));
        let mut h1 = HeaderMap::new();
        h1.insert("X-Server-Version",HeaderValue::from_static("2.1.0-alpha.0+git"));
        let tests =  vec![
          test_header{
            header: HeaderMap::new(),
            wv: Version::parse("2.0.0").unwrap(),
          },
            test_header{
                header: h,
                wv: Version::parse("2.1.0").unwrap(),
            },
            test_header{
                header: h1,
                wv: Version::parse("2.1.0-alpha.0+git").unwrap(),
            }
        ];
        for test in tests {
            assert_eq!(server_version(&test.header).unwrap(), test.wv);
        }
    }

    #[test]
    fn test_min_version(){
        let mut h= HeaderMap::new();
        h.insert("X-Min-Cluster-Version",HeaderValue::from_static("2.1.0"));
        let mut h1 = HeaderMap::new();
        h1.insert("X-Min-Cluster-Version",HeaderValue::from_static("2.1.0-alpha.0+git"));
        let tests =  vec![
            test_header{
                header: HeaderMap::new(),
                wv: Version::parse("2.0.0").unwrap(),
            },
            test_header{
                header: h,
                wv: Version::parse("2.1.0").unwrap(),
            },
            test_header{
                header: h1,
                wv: Version::parse("2.1.0-alpha.0+git").unwrap(),
            }
        ];
        for test in tests {
            assert_eq!(min_cluster_version(&test.header).unwrap(), test.wv);
        }
    }

    #[test]
    fn test_check_version_cmp(){
        let ls = Version::parse(version::Version).unwrap();
        let lmc = Version::parse(version::MinClusterVersion).unwrap();
        let tests = vec![
          test_version{
              server : ls.clone(),
              min_cluster: lmc.clone(),
              want: true,
          },
            test_version{
              server : lmc.clone(),
              min_cluster: Version::new(0,0,0),
              want: true,
          },
            test_version{
                server : Version::new(ls.clone().major+1,ls.clone().minor,ls.clone().patch),
                min_cluster: Version::new(0,0,0),
                want: true,
            },
            test_version{
                server : Version::new(lmc.clone().major-1,lmc.clone().minor,ls.clone().patch),
                min_cluster: Version::new(0,0,0),
                want: false,
            },
        ];
        for test in tests {
            assert_eq!(check_version_compatibility("",&test.server,&test.min_cluster).is_ok(),test.want)
        }
    }

    fn write_entry_to(ent: &Entry) -> io::Result<Vec<u8>> {
        let vec = Message::write_to_bytes(ent).unwrap();
        Ok(vec)
    }


    fn read_entry_from(entry:Vec<u8>) -> io::Result<Entry> {
        Ok(Message::parse_from_bytes(&entry).unwrap())
    }

    struct test{
        va:Version,
        vb:Version,
        want:i32,
    }

    struct test_header{
        header:HeaderMap,
        wv:Version,
    }

    struct test_version{
        server:Version,
        min_cluster:Version,
        want:bool,
    }
}