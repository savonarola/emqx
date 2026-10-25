use crate::{
    atoms,
    mqtt::{self, Message},
};
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use rustler::{Encoder, LocalPid, Monitor, OwnedEnv};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::{mpsc, watch, Notify};

#[derive(Clone, rustler::NifMap)]
pub struct Limits {
    pub representatives: usize,
    pub queue_limit: usize,
    pub batch_size: usize,
    pub output_limit: usize,
    pub max_packet_size: usize,
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
            worker_threads: 2,
        }
    }
}

pub enum Event {
    Subscribe(String),
    Unsubscribe(String),
    Publish(Arc<str>, Message),
}

#[derive(Default)]
pub struct Queue {
    items: VecDeque<Event>,
    armed: bool,
}

impl Queue {
    fn push(&mut self, item: Event) -> bool {
        self.items.push_back(item);
        let notify = !self.armed;
        self.armed = true;
        notify
    }

    fn take(&mut self, max: usize) -> (Vec<Event>, bool) {
        let batch = self.items.drain(..max.min(self.items.len())).collect();
        let more = !self.items.is_empty();
        if !more {
            self.armed = false;
        }
        (batch, more)
    }
}

pub enum Output {
    Packet(Bytes),
    Close,
}

pub struct Endpoint {
    pub id: u64,
    pub slot: usize,
    output: mpsc::Sender<Output>,
    pub close: watch::Sender<bool>,
    pub representative_alive: Arc<AtomicBool>,
}

impl Endpoint {
    pub fn send(&self, packet: &Bytes) {
        if self
            .output
            .try_send(Output::Packet(packet.clone()))
            .is_err()
        {
            self.close.send_replace(true);
        }
    }

    pub fn close_after_flush(&self) {
        if self.output.try_send(Output::Close).is_err() {
            self.close.send_replace(true);
        }
    }
}

type Members = HashMap<u64, Arc<Endpoint>>;

#[derive(Default)]
struct Router {
    exact: HashMap<String, Members>,
    wildcard: HashMap<String, Members>,
}

impl Router {
    fn entries(&mut self, filter: &str) -> &mut HashMap<String, Members> {
        if filter.contains(['+', '#']) {
            &mut self.wildcard
        } else {
            &mut self.exact
        }
    }

    fn add(&mut self, filter: &str, ep: &Arc<Endpoint>) {
        self.entries(filter)
            .entry(filter.to_owned())
            .or_default()
            .insert(ep.id, ep.clone());
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

    fn publish(&self, msg: &Message) {
        // Encode only when at least one filter matches.
        let mut packet = None;
        let mut send = |members: &Members| {
            let packet = packet
                .get_or_insert_with(|| mqtt::publish(msg.topic.as_bytes(), &msg.payload, false));
            for ep in members.values() {
                ep.send(packet);
            }
        };
        if let Some(members) = self.exact.get(&msg.topic) {
            send(members);
        }
        for (filter, members) in &self.wildcard {
            if matches(filter, &msg.topic) {
                send(members);
            }
        }
    }
}

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

#[derive(Default)]
pub struct SlotInner {
    pub representative: Option<(LocalPid, Monitor)>,
    pub alive: Arc<AtomicBool>,
    queue: Queue,
    members: HashMap<String, Members>,
    channels: HashMap<u64, (Arc<Endpoint>, HashSet<String>)>,
}

#[derive(Default)]
pub struct Slot {
    pub inner: Mutex<SlotInner>,
    pub size: AtomicUsize,
    drained: Notify,
}

pub struct NodeState {
    pub limits: Limits,
    pub slots: Vec<Slot>,
    router: RwLock<Router>,
    pub forwarding: AtomicBool,
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
            });
            inner.channels.insert(id, (ep.clone(), HashSet::new()));
            return Some((ep, receiver));
        }
        None
    }

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

    pub fn subscribe(&self, ep: &Arc<Endpoint>, filter: &str) -> bool {
        let mut inner = self.slots[ep.slot].inner.lock();
        let Some((_, filters)) = inner.channels.get_mut(&ep.id) else {
            return false;
        };
        if !filters.insert(filter.to_owned()) {
            return true;
        }
        self.router.write().add(filter, ep);
        let members = inner.members.entry(filter.to_owned()).or_default();
        let first = members.is_empty();
        members.insert(ep.id, ep.clone());
        if first {
            self.push(ep.slot, &mut inner, Event::Subscribe(filter.to_owned()));
        }
        true
    }

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

    pub fn unsubscribe(&self, ep: &Endpoint, filter: &str) {
        let mut inner = self.slots[ep.slot].inner.lock();
        if let Some((_, filters)) = inner.channels.get_mut(&ep.id) {
            if filters.remove(filter) {
                self.remove_filter(ep, filter, &mut inner);
            }
        }
    }

    pub fn detach(&self, ep: &Endpoint) {
        let mut inner = self.slots[ep.slot].inner.lock();
        if let Some((_, filters)) = inner.channels.remove(&ep.id) {
            for filter in filters {
                self.remove_filter(ep, &filter, &mut inner);
            }
        }
    }

    pub fn publish(&self, ep: &Endpoint, client: &Arc<str>, msg: Message, will: bool) -> bool {
        let slot = &self.slots[ep.slot];
        if !will && !ep.representative_alive.load(Ordering::Acquire) {
            return false;
        }
        self.router.read().publish(&msg);
        if self.forwarding.load(Ordering::Acquire) {
            let mut inner = slot.inner.lock();
            if ep.representative_alive.load(Ordering::Acquire) {
                self.push(ep.slot, &mut inner, Event::Publish(client.clone(), msg));
            }
        }
        true
    }

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

    pub fn deliver(&self, slot: usize, batch: &[(&str, Bytes)]) {
        let inner = self.slots[slot].inner.lock();
        for (filter, packet) in batch {
            if let Some(members) = inner.members.get(*filter) {
                for ep in members.values() {
                    ep.send(packet);
                }
            }
        }
    }

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
        assert_ne!(a.slot, b.slot);
        s.subscribe(&a, "a/#");
        s.subscribe(&a, "a/#");
        s.subscribe(&a, "a/+");
        s.subscribe(&b, "a/#");
        assert_eq!(s.take(a.slot, 10).0.len(), 2);
        let msg = Message {
            topic: "a/b".into(),
            payload: Bytes::new(),
            qos: 0,
        };
        s.publish(&a, &"a".into(), msg.clone(), false);
        assert!(ar.try_recv().is_ok());
        assert!(ar.try_recv().is_ok());
        assert!(ar.try_recv().is_err());
        assert!(br.try_recv().is_ok());
        s.deliver(b.slot, &[("a/#", mqtt::publish(b"a/b", b"", false))]);
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
        s.subscribe(&ep, "a");
        let msg = Message {
            topic: "a".into(),
            payload: Bytes::new(),
            qos: 0,
        };
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
        let msg = Message {
            topic: "a".into(),
            payload: Bytes::new(),
            qos: 0,
        };
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
