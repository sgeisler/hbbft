#![feature(deadline_api)]
#![feature(duration_float)]

extern crate bincode;
extern crate hbbft;
extern crate rand;

use hbbft::{NetworkInfo, Target};
use hbbft::dynamic_honey_badger::{DynamicHoneyBadgerBuilder, Message};
use hbbft::honey_badger::{EncryptionSchedule, SubsetHandlingStrategy};
use hbbft::queueing_honey_badger::*;

use rand::{OsRng, Rng, thread_rng};

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread::{JoinHandle, sleep, spawn};
use std::time::{Duration, Instant, SystemTime};


type Transaction = Vec<u8>;
type NodeId = usize;

const NODES: usize = 10;
const NODES_ROGUE: &[NodeId] = &[0];
const NODES_JOIN_LATER: &[NodeId] = &[1];
const JOIN_DELAY_SECS: u64 = 15;

const TX_SIZE: usize = 500;
const TX_COUNT: usize = 40000;
const MAX_BLOCK_SIZE: usize = 2000;

const NETWORK_DELAY_MS: u64 = 400;
const NETWORK_DELAY_TOLERANCE_MS: u64 = 5;

const ENCRYPTION_SCHEDULE: EncryptionSchedule = EncryptionSchedule::Always;
const SUBSET_STRATEGY: SubsetHandlingStrategy = SubsetHandlingStrategy::AllAtEnd;

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
}

#[derive(Debug)]
pub struct SimNet {
    send_sockets: Vec<Sender<DelayedMessage>>,
    receive_socket: Receiver<DelayedMessage>,
    running_nodes: Vec<JoinHandle<()>>,
    network_delay: Duration,
    network_counter: BTreeMap<NodeId, (usize, usize)>
}


pub fn unix_timestamp() -> f64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_float_secs()
}

pub fn is_rogue(node_id: &NodeId) -> bool {
    NODES_ROGUE.iter().any(|node| node == node_id)
}

pub fn is_joining_late(node_id: &NodeId) -> bool {
    NODES_JOIN_LATER.iter().any(|node| node == node_id)
}

impl SimNode {
    pub fn new(
        network_info: NetworkInfo<NodeId>,
        send_socket: Sender<DelayedMessage>,
        receive_socket: Receiver<DelayedMessage>,
        mempool: Vec<Transaction>
    ) -> SimNode {
        let id = network_info.our_id().clone();

        let dhb = DynamicHoneyBadgerBuilder::new()
            .encryption_schedule(ENCRYPTION_SCHEDULE)
            .subset_handling_strategy(SUBSET_STRATEGY)
            .build(network_info);

        let (qhb, first_step) = QueueingHoneyBadgerBuilder::new(dhb)
            .batch_size(MAX_BLOCK_SIZE)
            .queue(mempool)
            .build(OsRng::new().unwrap());

        let mut node = SimNode {
            id,
            hb: qhb,
            send_socket,
            receive_socket,
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
            self.send_socket.send(wire_msg);
        }
    }
}

impl SimNet {
    pub fn new() -> SimNet {
        println!("Starting SimNet");
        let (node_send, remote_receive) = channel::<DelayedMessage>();

        let mut rng = OsRng::new().unwrap();

        println!("Generating mempool");
        let mempool = (0..TX_COUNT).into_iter()
            .map(|_| rng.gen_iter().take(TX_SIZE).collect())
            .collect::<Vec<_>>();

        println!("Generating nodes");
        let network_infos = NetworkInfo::generate_map(0..NODES, &mut rng).unwrap();
        let (nodes, channels): (Vec<_>, Vec<_>) = network_infos
            .into_iter()
            .map(|(_, network_info)| {
                let (remote_send, node_receive) = channel::<DelayedMessage>();
                (SimNode::new(network_info, node_send.clone(), node_receive, mempool.clone()), remote_send)
            })
            .unzip();

        let threads = nodes.into_iter()
            .map(|node| {
                spawn(move || {
                    let mut node: SimNode = node;

                    if is_joining_late(&node.id()) {
                        sleep(Duration::from_secs(JOIN_DELAY_SECS + 1));
                    }

                    println!("{} spawning node {}", unix_timestamp(), node.id());
                    node.run();
                })
            })
            .collect::<Vec<_>>();

        SimNet {
            send_sockets: channels,
            receive_socket: remote_receive,
            running_nodes: threads,
            network_delay: Duration::from_millis(NETWORK_DELAY_MS),
            network_counter: (0..NODES).into_iter().map(|id| (id, (0, 0))).collect(),
        }
    }

    pub fn dispatch_messages(&mut self) {
        let mut rng = thread_rng();
        let mut start_time = Instant::now();
        let mut last_report_time = Instant::now();

        while let Ok(msg) = self.receive_socket.recv() {
            // simulate rogue nodes and late joiners
            if (is_rogue(&msg.sender) && rng.gen_weighted_bool(2)) ||
               (is_joining_late(&msg.sender) && start_time.elapsed() < Duration::from_secs(JOIN_DELAY_SECS)) {
                continue;
            }

            let msg_len = bincode::serialize(&msg.message).unwrap().len();
            self.network_counter.get_mut(&msg.sender).map(|x| x.0 += msg_len);

            // simulate network latency
            let since_sent = msg.send_time.elapsed();
            let target_delay = Duration::from_millis(NETWORK_DELAY_MS);
            if since_sent < target_delay && (target_delay - since_sent) > Duration::from_millis(NETWORK_DELAY_TOLERANCE_MS) {
                sleep(target_delay - since_sent)
            }

            match msg.receiver.clone() {
                Target::All => {
                    for (id, node_socket) in self.send_sockets.iter_mut().enumerate() {
                        if is_joining_late(&id) && start_time.elapsed() < Duration::from_secs(JOIN_DELAY_SECS) {
                            continue;
                        }
                        self.network_counter.get_mut(&id).map(|x| x.1 += msg_len);
                        node_socket.send(msg.clone());
                    }
                },
                Target::Node(receiver) => {
                    if !(is_joining_late(&receiver) && start_time.elapsed() < Duration::from_secs(JOIN_DELAY_SECS)) {
                        self.network_counter.get_mut(&receiver).map(|x| x.1 += msg_len);
                        self.send_sockets[receiver].send(msg);
                    }
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
    let mut net = SimNet::new();
    net.dispatch_messages();
}