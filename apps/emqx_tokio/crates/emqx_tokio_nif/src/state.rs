use crate::{
    atoms,
    mqtt::{Message, Subscription, Version},
};
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use rustler::{Encoder, LocalPid, Monitor, OwnedEnv};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, OnceLock,
    },
};
use tokio::sync::{mpsc, watch, Notify};

/// Runtime limits from the `emqx_tokio` application environment.
#[derive(Clone, rustler::NifMap)]
pub struct Limits {
    pub representatives: usize,
    pub queue_limit: usize,
    pub batch_size: usize,
    pub output_limit: usize,
    pub max_packet_size: usize,
    pub max_inflight: usize,
    pub worker_threads: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            representatives: 1,
            queue_limit: 8192,
            batch_size: 256,
            output_limit: 1024,
            max_packet_size: 1024 * 1024,
            max_inflight: 32,
            worker_threads: 2,
        }
    }
}

/// Events that a slot queues for its representative.
pub enum Event {
    Subscribe(String),
    Unsubscribe(String),
    Publish(Arc<str>, Message),
}

/// Representative queue. `armed` is true while a `native_ready` message is outstanding.
#[derive(Default)]
pub struct Queue {
    items: VecDeque<Event>,
    armed: bool,
}

impl Queue {
    /// Append an event. Returns true when the representative must be notified.
    fn push(&mut self, item: Event) -> bool {
        self.items.push_back(item);
        let notify = !self.armed;
        self.armed = true;
        notify
    }

    /// Remove up to `max` events. The queue is disarmed when it becomes empty.
    fn take(&mut self, max: usize) -> (Vec<Event>, bool) {
        let batch = self.items.drain(..max.min(self.items.len())).collect();
        let more = !self.items.is_empty();
        if !more {
            self.armed = false;
        }
        (batch, more)
    }
}

/// Items for the writer task of one connection.
pub enum Output {
    // CONNACK, and the receiver of PUBACK packet IDs from the reader.
    Connected(mpsc::Receiver<u16>, Bytes),
    // An encoded control packet.
    Packet(Bytes),
    // A PUBLISH to send at the given QoS. The writer adds the header and packet ID.
    Delivery(Message, u8),
    // PUBACK to the client for a QoS 1 PUBLISH it sent.
    Puback(u16),
    // Write everything queued before this item, then close.
    Close,
}

/// Connection settings fixed at CONNECT.
pub struct Connection {
    pub version: Version,
    pub maximum_packet_size: usize,
    pub window: usize,
}

/// The router's handle to one connection: its output queue and its close signal.
pub struct Endpoint {
    pub id: u64,
    pub slot: usize,
    output: mpsc::Sender<Output>,
    // Set to true to end the connection task at once, without flushing.
    pub close: watch::Sender<bool>,
    pub representative_alive: Arc<AtomicBool>,
    // Set once after CONNECT. Deliveries before that are dropped.
    pub connection: OnceLock<Connection>,
    // Items in the output queue and in the writer's pending queue, bounded by `output_limit`.
    queued: AtomicUsize,
    output_limit: usize,
}

impl Endpoint {
    /// Queue a control packet, unless it exceeds the client's Maximum Packet Size.
    pub fn send(&self, packet: &Bytes) {
        if self
            .connection
            .get()
            .is_none_or(|c| packet.len() <= c.maximum_packet_size)
        {
            self.enqueue(Output::Packet(packet.clone()));
        }
    }

    /// Queue an item. Close the connection when the output limit is reached (slow subscriber).
    pub fn enqueue(&self, item: Output) {
        if self.queued.fetch_add(1, Ordering::AcqRel) >= self.output_limit
            || self.output.try_send(item).is_err()
        {
            self.written(1);
            self.close.send_replace(true);
        }
    }

    /// Release output-limit credit for items that were written or not queued.
    pub fn written(&self, count: usize) {
        self.queued.fetch_sub(count, Ordering::AcqRel);
    }

    /// Queue a PUBLISH at min(message QoS, granted QoS) if it fits the client's packet size limit.
    fn deliver(&self, msg: &Message, granted: u8) {
        let Some(c) = self.connection.get() else {
            return;
        };
        let qos = msg.qos.min(granted);
        if msg.packet_size(c.version, qos) <= c.maximum_packet_size {
            self.enqueue(Output::Delivery(msg.clone(), qos));
        }
    }

    /// Close the connection after the writer has written the items queued before.
    pub fn close_after_flush(&self) {
        self.enqueue(Output::Close);
    }
}

/// One subscriber of a filter, with its subscription options.
#[derive(Clone)]
struct Member {
    endpoint: Arc<Endpoint>,
    options: Subscription,
}

/// The subscribers of one filter, by endpoint ID.
type Members = HashMap<u64, Member>;

/// Node-wide subscription table for native publishes. Wildcard filters are matched one by one.
#[derive(Default)]
struct Router {
    exact: HashMap<String, Members>,
    wildcard: HashMap<String, Members>,
}

impl Router {
    /// The map that holds this filter: wildcard or exact.
    fn entries(&mut self, filter: &str) -> &mut HashMap<String, Members> {
        if filter.contains(['+', '#']) {
            &mut self.wildcard
        } else {
            &mut self.exact
        }
    }

    fn add(&mut self, filter: &str, member: Member) {
        self.entries(filter)
            .entry(filter.to_owned())
            .or_default()
            .insert(member.endpoint.id, member);
    }

    fn remove(&mut self, filter: &str, id: u64) {
        let entries = self.entries(filter);
        if let Some(members) = entries.get_mut(filter) {
            members.remove(&id);
            if members.is_empty() {
                entries.remove(filter);
            }
        }
    }

    /// Queue a native publish to every matching member. No Local members skip their own publishes.
    fn publish(&self, origin: u64, msg: &Message) {
        let send = |members: &Members| {
            for member in members.values() {
                if !member.options.no_local || member.endpoint.id != origin {
                    member.endpoint.deliver(msg, member.options.qos);
                }
            }
        };
        if let Some(members) = self.exact.get(msg.topic()) {
            send(members);
        }
        for (filter, members) in &self.wildcard {
            if matches(filter, msg.topic()) {
                send(members);
            }
        }
    }
}

/// MQTT filter matching. Topics that start with `$` match only filters that start with `$`.
fn matches(filter: &str, topic: &str) -> bool {
    if topic.starts_with('$') && !filter.starts_with('$') {
        return false;
    }
    let mut topic = topic.split('/');
    for level in filter.split('/') {
        if level == "#" {
            return true;
        }
        match topic.next() {
            Some(t) if level == "+" || level == t => (),
            _ => return false,
        }
    }
    topic.next().is_none()
}

/// State of one representative slot, guarded by the slot mutex.
#[derive(Default)]
pub struct SlotInner {
    pub representative: Option<(LocalPid, Monitor)>,
    // Shared with the slot's endpoints. False once the representative is gone.
    pub alive: Arc<AtomicBool>,
    queue: Queue,
    // The slot's subscribers by filter, for broker deliveries. The broker has already matched them.
    members: HashMap<String, Members>,
    // The slot's connections and the filters each one subscribed.
    channels: HashMap<u64, (Arc<Endpoint>, HashSet<String>)>,
}

/// One slot: its state, its queue length, and a notifier for publishers waiting for capacity.
#[derive(Default)]
pub struct Slot {
    pub inner: Mutex<SlotInner>,
    pub size: AtomicUsize,
    drained: Notify,
}

/// State shared by all connections and NIF calls of one runtime.
pub struct NodeState {
    pub limits: Limits,
    pub slots: Vec<Slot>,
    router: RwLock<Router>,
    // When true, native publishes are also queued for the broker.
    pub forwarding: AtomicBool,
    // Set during shutdown, so that closing connections do not publish wills.
    pub quiet: AtomicBool,
    next_id: AtomicU64,
}

impl NodeState {
    pub fn new(limits: Limits) -> Self {
        Self {
            slots: (0..limits.representatives)
                .map(|_| Slot::default())
                .collect(),
            limits,
            router: RwLock::new(Router::default()),
            forwarding: AtomicBool::new(true),
            quiet: AtomicBool::new(false),
            next_id: AtomicU64::new(0),
        }
    }

    /// Register a new connection in the next slot that has a live representative (round robin).
    pub fn attach(&self) -> Option<(Arc<Endpoint>, mpsc::Receiver<Output>)> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        for offset in 0..self.slots.len() {
            let slot = (id as usize + offset) % self.slots.len();
            let mut inner = self.slots[slot].inner.lock();
            if !inner.alive.load(Ordering::Acquire) {
                continue;
            }
            let (output, receiver) = mpsc::channel(self.limits.output_limit);
            let (close, _) = watch::channel(false);
            let ep = Arc::new(Endpoint {
                id,
                slot,
                output,
                close,
                representative_alive: inner.alive.clone(),
                connection: OnceLock::new(),
                queued: AtomicUsize::new(0),
                output_limit: self.limits.output_limit,
            });
            inner.channels.insert(id, (ep.clone(), HashSet::new()));
            return Some((ep, receiver));
        }
        None
    }

    /// Queue an event for the slot's representative and wake it when the queue was not armed.
    fn push(&self, slot: usize, inner: &mut SlotInner, event: Event) {
        let notify = inner.queue.push(event);
        self.slots[slot]
            .size
            .store(inner.queue.items.len(), Ordering::Release);
        if notify {
            if let Some((pid, _)) = inner.representative {
                let _ =
                    OwnedEnv::new().send_and_clear(&pid, |env| atoms::native_ready().encode(env));
            }
        }
    }

    /// Add or update a subscription. A filter's first slot member subscribes the representative.
    pub fn subscribe(&self, ep: &Arc<Endpoint>, filter: &str, options: Subscription) -> bool {
        let mut inner = self.slots[ep.slot].inner.lock();
        let Some((_, filters)) = inner.channels.get_mut(&ep.id) else {
            return false;
        };
        filters.insert(filter.to_owned());
        let member = Member {
            endpoint: ep.clone(),
            options,
        };
        // The router serves native publishes; the slot members serve broker deliveries.
        self.router.write().add(filter, member.clone());
        let members = inner.members.entry(filter.to_owned()).or_default();
        let first = members.is_empty();
        members.insert(ep.id, member);
        if first {
            self.push(ep.slot, &mut inner, Event::Subscribe(filter.to_owned()));
        }
        true
    }

    /// Remove a subscription. The slot's last member of a filter unsubscribes the representative.
    fn remove_filter(&self, ep: &Endpoint, filter: &str, inner: &mut SlotInner) {
        self.router.write().remove(filter, ep.id);
        if let Some(members) = inner.members.get_mut(filter) {
            members.remove(&ep.id);
            if members.is_empty() {
                inner.members.remove(filter);
                self.push(ep.slot, inner, Event::Unsubscribe(filter.to_owned()));
            }
        }
    }

    /// Remove one subscription of a connection. Returns false when it did not exist.
    pub fn unsubscribe(&self, ep: &Endpoint, filter: &str) -> bool {
        let mut inner = self.slots[ep.slot].inner.lock();
        if let Some((_, filters)) = inner.channels.get_mut(&ep.id) {
            if filters.remove(filter) {
                self.remove_filter(ep, filter, &mut inner);
                return true;
            }
        }
        false
    }

    /// Remove a closed connection and all its subscriptions.
    pub fn detach(&self, ep: &Endpoint) {
        let mut inner = self.slots[ep.slot].inner.lock();
        if let Some((_, filters)) = inner.channels.remove(&ep.id) {
            for filter in filters {
                self.remove_filter(ep, &filter, &mut inner);
            }
        }
    }

    /// Deliver a native publish locally, and queue it for the broker when forwarding is on.
    pub fn publish(&self, ep: &Endpoint, client: &Arc<str>, msg: Message, will: bool) -> bool {
        let slot = &self.slots[ep.slot];
        // Without a live representative only wills are accepted; the caller closes the connection.
        if !will && !ep.representative_alive.load(Ordering::Acquire) {
            return false;
        }
        self.router.read().publish(ep.id, &msg);
        if self.forwarding.load(Ordering::Acquire) {
            let mut inner = slot.inner.lock();
            if ep.representative_alive.load(Ordering::Acquire) {
                self.push(ep.slot, &mut inner, Event::Publish(client.clone(), msg));
            }
        }
        true
    }

    /// Drain events for the representative. Wake waiting publishers once the queue is at the limit.
    pub fn take(&self, slot: usize, max: usize) -> (Vec<Event>, bool) {
        let slot = &self.slots[slot];
        let mut inner = slot.inner.lock();
        let before = inner.queue.items.len();
        let result = inner.queue.take(max.min(self.limits.batch_size));
        let after = inner.queue.items.len();
        slot.size.store(after, Ordering::Release);
        if before > self.limits.queue_limit && after <= self.limits.queue_limit {
            slot.drained.notify_waiters();
        }
        result
    }

    /// Wait until the slot's representative queue is at or below its limit.
    pub async fn wait_capacity(&self, slot: usize) {
        let slot = &self.slots[slot];
        loop {
            // Create the waiter before the check so that a notification between them is kept.
            let drained = slot.drained.notified();
            if slot.size.load(Ordering::Acquire) <= self.limits.queue_limit {
                return;
            }
            drained.await;
        }
    }

    /// Deliver broker messages to the slot's members of each filter.
    pub fn deliver(&self, slot: usize, batch: &[(&str, Message)]) {
        let inner = self.slots[slot].inner.lock();
        for (filter, msg) in batch {
            if let Some(members) = inner.members.get(*filter) {
                for member in members.values() {
                    member.endpoint.deliver(msg, member.options.qos);
                }
            }
        }
    }

    /// Reset a slot whose representative is gone or replaced. Closes all its connections.
    pub fn clear_slot(&self, index: usize, inner: &mut SlotInner) {
        inner.alive.store(false, Ordering::Release);
        let mut router = self.router.write();
        for (_, (ep, filters)) in inner.channels.drain() {
            for filter in filters {
                router.remove(&filter, ep.id);
            }
            ep.close.send_replace(true);
        }
        inner.members.clear();
        inner.queue = Queue::default();
        inner.representative = None;
        self.slots[index].size.store(0, Ordering::Release);
        self.slots[index].drained.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub fn connect(ep: &Endpoint) {
        let _ = ep.connection.set(Connection {
            version: Version::V4,
            maximum_packet_size: usize::MAX,
            window: 32,
        });
    }

    fn node() -> NodeState {
        let state = NodeState::new(Limits {
            representatives: 2,
            output_limit: 4,
            ..Limits::default()
        });
        for slot in &state.slots {
            slot.inner.lock().alive.store(true, Ordering::Release);
        }
        state.forwarding.store(false, Ordering::Release);
        state
    }

    #[test]
    fn wildcard_matching() {
        for (filter, topic, expected) in [
            ("a/#", "a", true),
            ("+", "", true),
            ("#", "$SYS/a", false),
            ("$SYS/#", "$SYS/a", true),
            ("a/+", "a/", true),
            ("a/+", "a", false),
            ("a/#", "ab", false),
        ] {
            assert_eq!(matches(filter, topic), expected, "{filter} {topic}");
        }
    }

    #[test]
    fn arming_and_batches() {
        let mut q = Queue::default();
        assert!(q.push(Event::Subscribe("a".into())));
        assert!(!q.push(Event::Unsubscribe("a".into())));
        assert!(q.take(1).1);
        assert!(!q.take(1).1);
        assert!(q.push(Event::Subscribe("b".into())));
    }

    #[test]
    fn membership_and_slot_delivery() {
        let s = node();
        let (a, mut ar) = s.attach().unwrap();
        let (b, mut br) = s.attach().unwrap();
        connect(&a);
        connect(&b);
        assert_ne!(a.slot, b.slot);
        s.subscribe(&a, "a/#", Subscription::default());
        s.subscribe(&a, "a/#", Subscription::default());
        s.subscribe(&a, "a/+", Subscription::default());
        s.subscribe(&b, "a/#", Subscription::default());
        assert_eq!(s.take(a.slot, 10).0.len(), 2);
        let msg = Message::copy(b"a/b", &[], &[], 0);
        s.publish(&a, &"a".into(), msg.clone(), false);
        assert!(ar.try_recv().is_ok());
        assert!(ar.try_recv().is_ok());
        assert!(ar.try_recv().is_err());
        assert!(br.try_recv().is_ok());
        s.deliver(b.slot, &[("a/#", msg)]);
        assert!(br.try_recv().is_ok());
        assert!(ar.try_recv().is_err());
        s.detach(&a);
        s.detach(&a);
        assert_eq!(s.take(a.slot, 10).0.len(), 2);
        assert!(s.slots[a.slot].inner.lock().members.is_empty());
    }

    #[test]
    fn output_bound() {
        let s = node();
        let (ep, _rx) = s.attach().unwrap();
        connect(&ep);
        s.subscribe(&ep, "a", Subscription::default());
        let msg = Message::copy(b"a", &[], &[], 0);
        for _ in 0..5 {
            assert!(s.publish(&ep, &"a".into(), msg.clone(), false));
        }
        assert!(*ep.close.borrow());
    }

    #[tokio::test]
    async fn publish_waits_for_queue_capacity() {
        let s = Arc::new(NodeState::new(Limits {
            queue_limit: 2,
            ..Limits::default()
        }));
        s.slots[0].inner.lock().alive.store(true, Ordering::Release);
        let (ep, _rx) = s.attach().unwrap();
        let msg = Message::copy(b"a", &[], &[], 0);
        // Publishes are accepted past the limit; only the next read waits.
        for _ in 0..3 {
            assert!(s.publish(&ep, &"a".into(), msg.clone(), false));
        }
        let wait = tokio::spawn({
            let s = s.clone();
            async move { s.wait_capacity(0).await }
        });
        tokio::task::yield_now().await;
        assert!(!wait.is_finished());
        // Draining the queue to the limit wakes the waiting publisher.
        s.take(0, 1);
        tokio::time::timeout(std::time::Duration::from_secs(1), wait)
            .await
            .unwrap()
            .unwrap();
    }
}
