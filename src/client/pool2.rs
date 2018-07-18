#![allow(dead_code)]
use futures::sync::oneshot;
use futures::{Async, Future, Poll};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use super::Ver;
use common::Exec;

pub(super) enum Conn<T, U> {
    Http1(T),
    Http2(U),
}

impl<T, U> Conn<T, U>
where
    T: Poolable,
    U: Poolable,
{
    fn is_open(&self) -> bool {
        match *self {
            Conn::Http1(ref v) => v.is_open(),
            Conn::Http2(ref v) => v.is_open(),
        }
    }

    fn ver(&self) -> Ver {
        match *self {
            Conn::Http1(_) => Ver::Http1,
            Conn::Http2(_) => Ver::Http2,
        }
    }
}

pub(super) struct Pool<T, U> {
    inner: Arc<PoolInner<T, U>>,
}

// Before using a pooled connection, make sure the sender is no dead.
//
// This is a trait to allow the `client::pool::tests` to work for `i32`.
//
// See https://github.com/hyperium/hyper/issues/1429
pub(super) trait Poolable: Send + Sized + 'static {
    fn is_open(&self) -> bool;
}

/// Simple type alias in case the key type needs to be adjusted.
type Key = Arc<String>;

struct PoolInner<T, U> {
    connections: Mutex<Connections<T, U>>,
    enabled: bool,
}

struct Connections<T, U> {
    // A flag that a connection is being established, and the connection
    // should be shared. This prevents making multiple HTTP/2 connections
    // to the same host.
    connecting: HashSet<Key>,
    // The HTTP protocol used by each host we've connected to recently.
    versions: HashMap<Key, Ver>,
    // These are internal Conns sitting in the event loop in the KeepAlive
    // state, waiting to receive a new Request to send on the socket.
    idle: HashMap<Key, IdleConnections<T, U>>,
    // These are outstanding Checkouts that are waiting for a socket to be
    // able to send a Request one. This is used when "racing" for a new
    // connection.
    //
    // The Client starts 2 tasks, 1 to connect a new socket, and 1 to wait
    // for the Pool to receive an idle Conn. When a Conn becomes idle,
    // this list is checked for any parked Checkouts, and tries to notify
    // them that the Conn could be used instead of waiting for a brand new
    // connection.
    waiters: HashMap<Key, VecDeque<oneshot::Sender<Conn<T, U>>>>,
    // A oneshot channel is used to allow the interval to be notified when
    // the Pool completely drops. That way, the interval can cancel immediately.
    #[cfg(feature = "runtime")]
    idle_interval_ref: Option<oneshot::Sender<::common::Never>>,
    #[cfg(feature = "runtime")]
    exec: Exec,
    timeout: Option<Duration>,
}

enum IdleConnections<T, U> {
    Http1(Vec<Idle<T>>),
    Http2(Idle<U>),
}

// This is because `Weak::new` *allocates* space for `T`, even if it
// doesn't need it!
struct WeakOpt<T>(Option<Weak<T>>);

impl<T, U> Pool<T, U> {
    pub fn new(enabled: bool, timeout: Option<Duration>, __exec: &Exec) -> Pool<T, U> {
        Pool {
            inner: Arc::new(PoolInner {
                connections: Mutex::new(Connections {
                    connecting: HashSet::new(),
                    versions: HashMap::new(),
                    idle: HashMap::new(),
                    #[cfg(feature = "runtime")]
                    idle_interval_ref: None,
                    waiters: HashMap::new(),
                    #[cfg(feature = "runtime")]
                    exec: __exec.clone(),
                    timeout,
                }),
                enabled,
            }),
        }
    }

    #[cfg(test)]
    pub(super) fn no_timer(&self) {
        // Prevent an actual interval from being created for this pool...
        #[cfg(feature = "runtime")]
        {
            let mut inner = self.inner.connections.lock().unwrap();
            assert!(inner.idle_interval_ref.is_none(), "timer already spawned");
            let (tx, _) = oneshot::channel();
            inner.idle_interval_ref = Some(tx);
        }
    }
}

impl<T, U> Pool<T, U>
where
    T: Poolable,
    U: Poolable + Clone,
{
    /// Returns a `Checkout` which is a future that resolves if an idle
    /// connection becomes available.
    pub fn checkout(&self, key: Key) -> Checkout<T, U> {
        Checkout {
            key,
            pool: self.clone(),
            waiter: None,
        }
    }

    /// Ensure there is only ever 1 connecting task for (potential) HTTP/2
    /// connections. This does nothing if we know the server uses HTTP/1.
    pub(super) fn connecting(&self, key: &Key) -> Option<Connecting<T, U>> {
        let mut inner = self.inner.connections.lock().unwrap();
        if self.inner.enabled && inner.versions.get(key) != Some(&Ver::Http1) {
            if inner.connecting.insert(key.clone()) {
                let connecting = Connecting {
                    key: key.clone(),
                    pool: WeakOpt::downgrade(&self.inner),
                };
                Some(connecting)
            } else {
                trace!(
                    "possible HTTP/2 connecting already in progress for {:?}",
                    key
                );
                None
            }
        } else {
            Some(Connecting {
                key: key.clone(),
                // in HTTP/1's case, there is never a lock, so we don't
                // need to do anything in Drop.
                pool: WeakOpt::none(),
            })
        }
    }

    #[cfg(feature = "runtime")]
    #[cfg(test)]
    pub(super) fn idle_count(&self, key: &Key) -> usize {
        self.inner
            .connections
            .lock()
            .unwrap()
            .idle
            .get(key)
            .map(|conns| match *conns {
                IdleConnections::Http1(ref values) => values.len(),
                IdleConnections::Http2(_) => 1,
            })
            .unwrap_or(0)
    }

    fn take(&self, key: &Key) -> Option<Pooled<T, U>> {
        let entry = {
            let mut inner = self.inner.connections.lock().unwrap();
            let expiration = Expiration::new(inner.timeout);
            let maybe_entry = inner.idle.get_mut(key).and_then(|conns| {
                trace!("take? {:?}: expiration = {:?}", key, expiration.0);
                if !conns.evict(key, &expiration) {
                    return None;
                }
                conns.get().map(|e| (e, conns.is_empty()))
            });
            let (entry, empty) = if let Some((e, empty)) = maybe_entry {
                (Some(e), empty)
            } else {
                (None, true)
            };
            if empty {
                // TOOD: This could be done with the HashMap::entry API instead.
                inner.idle.remove(key);
            }
            entry
        };

        entry.map(|e| self.reuse(key, e))
    }

    pub(super) fn pooled(
        &self,
        mut connecting: Connecting<T, U>,
        value: Conn<T, U>,
    ) -> Pooled<T, U> {
        let pool_ref = if self.inner.enabled {

        }
    }

    fn reuse(&self, key: &Key, value: Conn<T, U>) -> Pooled<T, U> {
        debug!("reuse idle connection for {:?}", key);
        let pool_ref = if value.ver() == Ver::Http2 {
            WeakOpt::none()
        } else {
            WeakOpt::downgrade(&self.inner)
        };

        Pooled {
            is_reused: true,
            key: key.clone(),
            pool: pool_ref,
            value: Some(value),
        }
    }

    fn waiter(&mut self, key: Key, tx: oneshot::Sender<Conn<T, U>>) {
        trace!("checkout waiting for idle connection: {:?}", key);
        self.inner
            .connections
            .lock()
            .unwrap()
            .waiters
            .entry(key)
            .or_insert(VecDeque::new())
            .push_back(tx);
    }
}

impl<T, U> Connections<T, U>
where
    T: Poolable,
    U: Poolable + Clone,
{
    fn put(&mut self, key: Key, value: Conn<T, U>, __pool_ref: &Arc<PoolInner<T, U>>) {
        trace!("put; add idle connection for {:?}", key);

        let (remove_idle, remove_waiters) = {
            let idle = match self.idle.entry(key.clone()) {
                Entry::Occupied(mut e) => {
                    e.get_mut().put(&key, value);
                    e.into_mut()
                }
                Entry::Vacant(e) => e.insert(IdleConnections::new(value)),
            };
            let mut remove_waiters = false;
            if let Some(waiters) = self.waiters.get_mut(&key) {
                while let Some(tx) = waiters.pop_front() {
                    if !tx.is_canceled() {
                        let conn = idle.get().expect("empty idle connections");
                        match tx.send(conn) {
                            Ok(()) => {
                                if idle.is_empty() {
                                    break;
                                } else {
                                    continue;
                                }
                            }
                            Err(e) => idle.put(&key, e),
                        }
                    }

                    trace!("put; removing canceled waiter for {:?}", key);
                }
                remove_waiters = waiters.is_empty();
            }
            (idle.is_empty(), remove_waiters)
        };

        if remove_idle {
            self.idle.remove(&key);
        }
        if remove_waiters {
            self.waiters.remove(&key);
        }
    }

    /// A `Connecting` task is complete. But not necessarily successfully,
    /// but the lock is going away, so clean up.
    fn connected(&mut self, key: &Key) {
        let existed = self.connecting.remove(key);
        debug_assert!(existed, "Connecting dropped, key not in pool.connecting");
        // cancel any waiters. if there are any, it's because
        // this Connecting task didn't complete successfully.
        // these waiters would never receive a connection.
        self.waiters.remove(key);
    }
}

impl<T, U> IdleConnections<T, U>
where
    T: Poolable,
    U: Poolable + Clone,
{
    fn new(value: Conn<T, U>) -> IdleConnections<T, U> {
        match value {
            Conn::Http1(value) => IdleConnections::Http1(vec![Idle {
                idle_at: Instant::now(),
                value,
            }]),
            Conn::Http2(value) => IdleConnections::Http2(Idle {
                idle_at: Instant::now(),
                value,
            }),
        }
    }

    fn put(&mut self, key: &Key, value: Conn<T, U>) {
        match value {
            Conn::Http1(value) => match *self {
                IdleConnections::Http1(ref mut values) => {
                    debug!("put; pooling idle HTTP/1 connection for {:?}", key);
                    values.push(Idle {
                        idle_at: Instant::now(),
                        value,
                    });
                }
                IdleConnections::Http2(_) => {
                    debug!("put; discarding idle HTTP/1 connection with existing HTTP/2 connection for {:?}", key);
                }
            },
            Conn::Http2(value) => match *self {
                IdleConnections::Http1(_) => {
                    debug!(
                        "put; replacing HTTP/1 connections with HTTP/2 connection for {:?}",
                        key
                    );
                    *self = IdleConnections::Http2(Idle {
                        idle_at: Instant::now(),
                        value,
                    });
                }
                IdleConnections::Http2(_) => {
                    debug!("put; got a new HTTP/2 connection with an existing HTTP/2 connection for {:?}", key);
                }
            },
        }
    }

    fn get(&mut self) -> Option<Conn<T, U>> {
        match *self {
            IdleConnections::Http1(ref mut values) => {
                values.pop().map(|i| i.value).map(Conn::Http1)
            }
            IdleConnections::Http2(ref mut value) => {
                value.idle_at = Instant::now();
                Some(Conn::Http2(value.value.clone()))
            }
        }
    }

    fn evict(&mut self, key: &Key, expiration: &Expiration) -> bool {
        match *self {
            IdleConnections::Http1(ref mut values) => {
                values.retain(|value| {
                    if !value.value.is_open() {
                        trace!("removing closed connection for {:?}", key);
                        return false;
                    }

                    if expiration.expires(value.idle_at) {
                        trace!("removing expired connection for {:?}", key);
                        return false;
                    }

                    true
                });
                !values.is_empty()
            }
            IdleConnections::Http2(ref value) => {
                if !value.value.is_open() {
                    trace!("removing closed connection for {:?}", key);
                    return false;
                }

                if expiration.expires(value.idle_at) {
                    trace!("removing expired connection for {:?}", key);
                    return false;
                }

                true
            }
        }
    }

    fn is_empty(&self) -> bool {
        match *self {
            IdleConnections::Http1(ref values) => values.is_empty(),
            IdleConnections::Http2(_) => false,
        }
    }
}

impl<T, U> Clone for Pool<T, U> {
    fn clone(&self) -> Pool<T, U> {
        Pool {
            inner: self.inner.clone(),
        }
    }
}

/// A wrapped poolable value that tries to reinsert to the Pool on Drop.
// Note: The bounds are needed for the `Drop` impl.
pub(super) struct Pooled<T, U>
where
    T: Poolable,
    U: Poolable + Clone,
{
    value: Option<Conn<T, U>>,
    is_reused: bool,
    key: Key,
    pool: WeakOpt<PoolInner<T, U>>,
}

impl<T, U> Pooled<T, U>
where
    T: Poolable,
    U: Poolable + Clone,
{
    pub fn is_reused(&self) -> bool {
        self.is_reused
    }

    pub fn is_pool_enabled(&self) -> bool {
        self.pool.0.is_some()
    }

    pub fn as_ref(&self) -> &Conn<T, U> {
        self.value.as_ref().expect("not dropped")
    }

    pub fn as_mut(&mut self) -> &mut Conn<T, U> {
        self.value.as_mut().expect("not dropped")
    }
}

impl<T, U> Deref for Pooled<T, U>
where
    T: Poolable,
    U: Poolable + Clone,
{
    type Target = Conn<T, U>;
    fn deref(&self) -> &Conn<T, U> {
        self.as_ref()
    }
}

impl<T, U> DerefMut for Pooled<T, U>
where
    T: Poolable,
    U: Poolable + Clone,
{
    fn deref_mut(&mut self) -> &mut Conn<T, U> {
        self.as_mut()
    }
}

impl<T, U> Drop for Pooled<T, U>
where
    T: Poolable,
    U: Poolable + Clone,
{
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            if !value.is_open() {
                // If we *already* know the connection is done here,
                // it shouldn't be re-inserted back into the pool.
                return;
            }

            if let Some(pool) = self.pool.upgrade() {
                // Pooled should not have a real reference if pool is
                // not enabled!
                debug_assert!(pool.enabled);

                if let Ok(mut inner) = pool.connections.lock() {
                    inner.put(self.key.clone(), value, &pool);
                }
            } else if let Conn::Http1(_) = value {
                trace!("pool dropped, dropping pooled ({:?})", self.key);
            }
            // Conn::Http2 is already in the Pool (or dead), so we woudln't
            // have an actual reference to the Pool.
        }
    }
}

struct Idle<T> {
    idle_at: Instant,
    value: T,
}

pub(super) struct Checkout<T, U> {
    key: Key,
    pool: Pool<T, U>,
    waiter: Option<oneshot::Receiver<Conn<T, U>>>,
}

impl<T, U> Checkout<T, U>
where
    T: Poolable,
    U: Poolable + Clone,
{
    fn poll_waiter(&mut self) -> Poll<Option<Pooled<T, U>>, ::Error> {
        const CANCELED: &str = "pool checkout failed";
        if let Some(mut rx) = self.waiter.take() {
            match rx.poll() {
                Ok(Async::Ready(value)) => {
                    if value.is_open() {
                        Ok(Async::Ready(Some(self.pool.reuse(&self.key, value))))
                    } else {
                        Err(::Error::new_canceled(Some(CANCELED)))
                    }
                }
                Ok(Async::NotReady) => {
                    self.waiter = Some(rx);
                    Ok(Async::NotReady)
                }
                Err(_canceled) => Err(::Error::new_canceled(Some(CANCELED))),
            }
        } else {
            Ok(Async::Ready(None))
        }
    }

    fn add_waiter(&mut self) {
        if self.waiter.is_none() {
            let (tx, mut rx) = oneshot::channel();
            let _ = rx.poll(); // park this task
            self.pool.waiter(self.key.clone(), tx);
            self.waiter = Some(rx);
        }
    }
}

impl<T, U> Future for Checkout<T, U>
where
    T: Poolable,
    U: Poolable + Clone,
{
    type Item = Pooled<T, U>;
    type Error = ::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Some(pooled) = try_ready!(self.poll_waiter()) {
            return Ok(Async::Ready(pooled));
        }

        let entry = self.pool.take(&self.key);

        if let Some(pooled) = entry {
            Ok(Async::Ready(pooled))
        } else {
            self.add_waiter();
            Ok(Async::NotReady)
        }
    }
}

pub(super) struct Connecting<T, U>
where
    T: Poolable,
    U: Poolable + Clone,
{
    key: Key,
    pool: WeakOpt<PoolInner<T, U>>,
}

impl<T, U> Drop for Connecting<T, U>
where
    T: Poolable,
    U: Poolable + Clone,
{
    fn drop(&mut self) {
        if let Some(pool) = self.pool.upgrade() {
            // No need to panic on drop, that could abort!
            if let Ok(mut inner) = pool.connections.lock() {
                inner.connected(&self.key);
            }
        }
    }
}

struct Expiration(Option<Duration>);

impl Expiration {
    fn new(dur: Option<Duration>) -> Expiration {
        Expiration(dur)
    }

    fn expires(&self, instant: Instant) -> bool {
        match self.0 {
            Some(timeout) => instant.elapsed() > timeout,
            None => false,
        }
    }
}

impl<T> WeakOpt<T> {
    fn none() -> Self {
        WeakOpt(None)
    }

    fn downgrade(arc: &Arc<T>) -> Self {
        WeakOpt(Some(Arc::downgrade(arc)))
    }

    fn upgrade(&self) -> Option<Arc<T>> {
        self.0.as_ref().and_then(Weak::upgrade)
    }
}
