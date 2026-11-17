use crate::graph_system;
use crate::tcp_stream_writer::TcpStreamWriter;
use mini_redis::{client, Result};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::vec::Vec;
use tklog::{error, info};
pub use uuid::Uuid;

pub trait RustMQInterface {
    fn rustmq_init_queue(&mut self, addr: String, msg: String) -> io::Result<&Vec<u8>>;
    fn rustmq_create_queue(&mut self) -> &VecDeque<Vec<u8>>;
    fn rustmq_get_queue(&self) -> &VecDeque<Vec<u8>>;
    fn rustmq_set_queue(&mut self, msg_queue: VecDeque<Vec<u8>>);
    fn rustmq_stream(&self);
    fn rustmq_write(&mut self, message: Vec<u8>);
    fn rustmq_read(&self);
}

pub struct DataPackage {
    pub msg: Vec<u8>,
    pub uuid: String,
    pub msg_queue: VecDeque<Vec<u8>>,
}

pub struct RustMQ {
    pub data_pkg: DataPackage,
}

#[tokio::main]
impl RustMQInterface for RustMQ {
    async fn rustmq_init_queue(&mut self, addr: String, msg: String) -> io::Result<&Vec<u8>> {
        info!("RustMQ Stream created, ", addr);
        let mut client = client::connect(addr).await?;
        self.data_pkg.uuid = Uuid::new_v4().to_string();
        self.data_pkg.msg_queue.push_back(client.get(""));
        self.data_pkg.msg = tcp_stream_writer.read_message().unwrap();
        println!("the message from client: {:?}", &self.data_pkg.msg_queue[0]);
        info!("message in queue");
        Ok(&self.data_pkg.msg)
    }

    fn rustmq_create_queue(&mut self) -> &VecDeque<Vec<u8>> {
        let data: VecDeque<Vec<u8>> = VecDeque::new();
        self.data_pkg.msg_queue = data;

        return &self.data_pkg.msg_queue;
    }

    fn rustmq_get_queue(&self) -> &VecDeque<Vec<u8>> {
        return &self.data_pkg.msg_queue;
    }

    fn rustmq_set_queue(&mut self, msg_queue: VecDeque<Vec<u8>>) {
        self.data_pkg.msg_queue = msg_queue;
    }

    fn rustmq_stream(&self) {}

    fn rustmq_write(&mut self, message: Vec<u8>) {
        self.data_pkg.msg = message;
    }

    fn rustmq_read(&self) {}
}
