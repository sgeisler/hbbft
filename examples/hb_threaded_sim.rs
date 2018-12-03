#![feature(deadline_api)]
#![feature(duration_float)]

extern crate bincode;
extern crate hbbft;
extern crate rand;
extern crate structopt;

use hbbft::{NetworkInfo, Target};
use hbbft::dynamic_honey_badger::{DynamicHoneyBadgerBuilder, Message};
use hbbft::honey_badger::{EncryptionSchedule, SubsetHandlingStrategy};
use hbbft::queueing_honey_badger::*;

use rand::{OsRng, Rng, thread_rng};

use std::collections::BTreeMap;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread::{JoinHandle, sleep, spawn};
use std::time::{Duration, Instant, SystemTime};

use structopt::StructOpt;

type Transaction = Vec<u8>;
type NodeId = usize;

const ENCRYPTION_SCHEDULE: EncryptionSchedule = EncryptionSchedule::Always;
const SUBSET_STRATEGY: SubsetHandlingStrategy = SubsetHandlingStrategy::AllAtEnd;

#[derive(Clone, Debug, StructOpt)]
#[structopt(name = "hb_threaded_sim", about = "Multi threaded simulation of a HBBFT network.")]
pub struct Config {
    #[structopt(short = "n", long = "node-count", default_value = "10")]
    nodes: usize,

    #[structopt(short = "r", long = "rogue-node-count", default_value = "2")]
    rogue: usize,

    #[structopt(short = "s", long = "tx-size", default_value = "500")]
    tx_size: usize,

    #[structopt(short = "b", long = "tx-per-block", default_value = "2000")]
    tx_per_block: usize,

    #[structopt(short = "m", long = "mempool-size", default_value = "20000")]
    mempool_size: usize,

    #[structopt(short = "l", long = "network-latency-ms", default_value = "500")]
    latency: u64,

    #[structopt(long = "broadcast-only")]
    broadcast_only: bool,
}

#[derive(Debug, Clone)]
pub struct DelayedMessage {
    send_time: Instant,
    receiver: Target<NodeId>,
    sender: NodeId,
    message: Message<usize>,
}

#[derive(Debug)]
pub struct SimNode {
    id: NodeId,
    hb: QueueingHoneyBadger<Transaction, NodeId, Vec<Transaction>>,
    send_socket: Sender<DelayedMessage>,
    receive_socket: Receiver<DelayedMessage>,
    config: Config,
}

#[derive(Debug)]
pub struct SimNet {
    send_sockets: Vec<Sender<DelayedMessage>>,
    receive_socket: Receiver<DelayedMessage>,
    running_nodes: Vec<JoinHandle<()>>,
    network_counter: BTreeMap<NodeId, (usize, usize)>,
    config: Config,
}


pub fn unix_timestamp() -> f64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_float_secs()
}

impl Config {
    pub fn is_rogue(&self, node_id: &NodeId) -> bool {
        *node_id < self.rogue
    }
}

impl SimNode {
    pub fn new(
        network_info: NetworkInfo<NodeId>,
        send_socket: Sender<DelayedMessage>,
        receive_socket: Receiver<DelayedMessage>,
        mempool: Vec<Transaction>,
        cfg: Config,
    ) -> SimNode {
        let id = network_info.our_id().clone();

        let dhb = DynamicHoneyBadgerBuilder::new()
            .encryption_schedule(ENCRYPTION_SCHEDULE)
            .subset_handling_strategy(SUBSET_STRATEGY)
            .build(network_info);

        let (qhb, first_step) = QueueingHoneyBadgerBuilder::new(dhb)
            .batch_size(cfg.tx_per_block)
            .queue(mempool)
            .build(OsRng::new().unwrap());

        let mut node = SimNode {
            id,
            hb: qhb,
            send_socket,
            receive_socket,
            config: cfg,
        };
        node.do_step(first_step);
        node
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn run(&mut self) {
        loop {
            if let Ok(msg) = self.receive_socket.recv_deadline(Instant::now() + Duration::from_millis(10)) {
                let step = self.hb.handle_message(
                    &msg.sender,
                    msg.message
                ).expect("let's hope that works");

                self.do_step(step);
            }
        }
    }

    fn do_step(&mut self, step: Step<Transaction, usize>) {
        if !step.output.is_empty() {
            println!("{} node {} epoch {}", unix_timestamp(), self.id(), step.output[0].epoch());
        }

        for msg in step.messages {
            let wire_msg = DelayedMessage {
                send_time: Instant::now(),
                receiver: msg.target,
                sender: self.id(),
                message: msg.message
            };
            self.send_socket.send(wire_msg).unwrap();
        }
    }
}

impl SimNet {
    pub fn new(cfg: Config) -> SimNet {
        println!("Starting SimNet");
        let (node_send, remote_receive) = channel::<DelayedMessage>();

        let mut rng = OsRng::new().unwrap();

        println!("Generating mempool");
        let mempool = (0..cfg.mempool_size).into_iter()
            .map(|_| rng.gen_iter().take(cfg.tx_size).collect())
            .collect::<Vec<_>>();

        println!("Generating nodes");
        let network_infos = NetworkInfo::generate_map(0..cfg.nodes, &mut rng).unwrap();
        let (nodes, channels): (Vec<_>, Vec<_>) = network_infos
            .into_iter()
            .map(|(_, network_info)| {
                let (remote_send, node_receive) = channel::<DelayedMessage>();
                (SimNode::new(network_info, node_send.clone(), node_receive, mempool.clone(), cfg.clone()), remote_send)
            })
            .unzip();

        let threads = nodes.into_iter()
            .map(|node| {
                spawn(move || {
                    let mut node: SimNode = node;
                    println!("{} spawning node {}", unix_timestamp(), node.id());
                    node.run();
                })
            })
            .collect::<Vec<_>>();

        SimNet {
            send_sockets: channels,
            receive_socket: remote_receive,
            running_nodes: threads,
            network_counter: (0..cfg.nodes).into_iter().map(|id| (id, (0, 0))).collect(),
            config: cfg,
        }
    }

    pub fn dispatch_messages(&mut self) {
        let mut rng = thread_rng();
        let start_time = Instant::now();
        let mut last_report_time = Instant::now();

        while let Ok(msg) = self.receive_socket.recv() {
            // simulate rogue nodes
            if self.config.is_rogue(&msg.sender) && rng.gen_weighted_bool(2) {
                continue;
            }

            let msg_len = bincode::serialize(&msg.message).unwrap().len();
            let node_count = self.config.nodes;

            // simulate network latency
            let since_sent = msg.send_time.elapsed();
            let target_delay = Duration::from_millis(self.config.latency);
            if since_sent < target_delay {
                sleep(target_delay - since_sent)
            }

            match msg.receiver.clone() {
                Target::All => {
                    self.network_counter.get_mut(&msg.sender).map(|x| x.0 += msg_len * node_count);
                    for (id, node_socket) in self.send_sockets.iter_mut().enumerate() {
                        self.network_counter.get_mut(&id).map(|x| x.1 += msg_len);
                        node_socket.send(msg.clone()).unwrap();
                    }
                },
                Target::Node(receiver) => {
                    if self.config.broadcast_only {
                        self.network_counter.get_mut(&msg.sender).map(|x| x.0 += msg_len * node_count);
                        self.network_counter.get_mut(&receiver).map(|x| x.1 += msg_len * node_count);
                    } else {
                        self.network_counter.get_mut(&msg.sender).map(|x| x.0 += msg_len);
                        self.network_counter.get_mut(&receiver).map(|x| x.1 += msg_len);
                    }
                    self.send_sockets[receiver].send(msg).unwrap();
                },
            }

            if last_report_time.elapsed() > Duration::from_secs(5) {
                let secs_since_start = start_time.elapsed().as_secs() as usize;
                for (node, (out_bytes, in_bytes)) in self.network_counter.iter() {
                    println!(
                        "node {} network: {} kbit/s out; {} kbit/sec in (since start)",
                        node,
                        out_bytes * 8 / secs_since_start / 1000,
                        in_bytes * 8 / secs_since_start / 1000
                    );
                }
                last_report_time = Instant::now();
            }
        }
    }
}

fn main() {
    let config = Config::from_args();

    let mut net = SimNet::new(config);
    net.dispatch_messages();
}