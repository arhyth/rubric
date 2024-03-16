use std::{
    collections::VecDeque,
    ops::{
        Deref,
        DerefMut,
    },
    sync::{
        Arc,
        Mutex,
    },
};

pub struct Pool<T>
where
    T: Sync + Send,
{
    inner: Arc<Mutex<PoolInner<T>>>,
}

struct PoolInner<T>
where
    T: Sync + Send,
{
    pool: VecDeque<T>,
}

pub struct Managed<T>
where
    T: Sync + Send,
{
    pool: Arc<Mutex<PoolInner<T>>>,
    item: T,
}

impl<T> Pool<T>
where
    T: Sync + Send,
{
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(PoolInner {
                pool: VecDeque::with_capacity(capacity),
            })),
        }
    }

    pub fn acquire<C: Fn() -> T>(&self, creator: C) -> Managed<T> {
        let pool_ref = self.inner.clone();

        let mut inner = self.inner.lock().expect("Pool could not be locked");

        while !inner.pool.is_empty() {
            if let Some(item) = inner.pool.pop_front() {
                return Managed {
                    pool: pool_ref,
                    item,
                };
            }
        }

        Managed {
            pool: pool_ref,
            item: creator(),
        }
    }
}

impl<T> Managed<Vec<T>>
where
    T: Sync + Send,
{
    pub fn recycle(mut self) {
        let mut inner = self.pool.lock().expect("Pool could not be locked");
        self.item.clear();
        inner.pool.push_back(self.item);
    }
}

impl<T> Deref for Managed<T>
where
    T: Sync + Send,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.item
    }
}

impl<T> DerefMut for Managed<T>
where
    T: Sync + Send,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.item
    }
}
