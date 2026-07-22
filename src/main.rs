use futures::future::poll_fn;
use pin_project_lite::pin_project;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::{Mutex as StdMutex, MutexGuard as StdMutexGuard, TryLockError};
use std::task::{Context, Poll, Waker};
use tokio::time::{Duration, sleep};

trait AsyncIterator {
    type Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollNext<Self::Item>;

    fn poll_progress(self: Pin<&mut Self>, cx: &mut Context) -> Poll<()>;

    /// Wrap an `AsyncIterator` so that it can be polled again after returning `Done`. Internally,
    /// the iterator is dropped the first time `poll_next` returns `Done`, and subsequent polls
    /// return `Done` or `Ready` again.
    fn fuse(self) -> Fuse<Self>
    where
        Self: Sized,
    {
        Fuse { iter: Some(self) }
    }

    fn merge<Other>(self, other: Other) -> Merge<Self, Other>
    where
        Self: Sized,
        Other: AsyncIterator<Item = Self::Item>,
    {
        Merge {
            left: self.fuse(),
            right: other.fuse(),
            poll_next_never_called: true,
            left_is_current: true,
            buffered: None,
        }
    }

    fn for_each<F, Fut>(self, f: F) -> ForEach<Self, F, Fut>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> Fut,
        Fut: Future<Output = ()>,
    {
        ForEach {
            iter: self,
            f,
            fut: None,
            progress_pending: false,
        }
    }
}

enum PollNext<Item> {
    Item(Item),
    Pending,
    Done,
}

pin_project! {
    #[must_use]
    struct Fuse<Iter> {
        #[pin]
        iter: Option<Iter>,
    }
}

impl<Iter> Fuse<Iter> {
    fn is_done(&self) -> bool {
        self.iter.is_none()
    }
}

impl<Iter> AsyncIterator for Fuse<Iter>
where
    Iter: AsyncIterator,
{
    type Item = Iter::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollNext<Iter::Item> {
        let mut this = self.project();
        match this.iter.as_mut().as_pin_mut() {
            Some(iter) => match iter.poll_next(cx) {
                PollNext::Item(item) => PollNext::Item(item),
                PollNext::Done => {
                    this.iter.set(None);
                    PollNext::Done
                }
                PollNext::Pending => PollNext::Pending,
            },
            None => PollNext::Done,
        }
    }

    fn poll_progress(self: Pin<&mut Self>, cx: &mut Context) -> Poll<()> {
        let this = self.project();
        match this.iter.as_pin_mut() {
            Some(iter) => iter.poll_progress(cx),
            None => Poll::Ready(()),
        }
    }
}

fn any_pending(polls: impl IntoIterator<Item = Poll<()>>) -> Poll<()> {
    for poll in polls {
        if poll.is_pending() {
            return Poll::Pending;
        }
    }
    Poll::Ready(())
}

fn update_pending(ret: &mut Poll<()>, poll: Poll<()>) {
    if poll.is_pending() {
        *ret = Poll::Pending;
    }
}

enum EitherPin<'a, S1, S2> {
    Left(Pin<&'a mut S1>),
    Right(Pin<&'a mut S2>),
}

impl<'a, S1, S2> EitherPin<'a, S1, S2>
where
    S1: AsyncIterator,
    S2: AsyncIterator<Item = S1::Item>,
{
    fn poll_next(self, cx: &mut Context) -> PollNext<S1::Item> {
        match self {
            EitherPin::Left(s1) => s1.poll_next(cx),
            EitherPin::Right(s2) => s2.poll_next(cx),
        }
    }

    fn poll_progress(self, cx: &mut Context) -> Poll<()> {
        match self {
            EitherPin::Left(s1) => s1.poll_progress(cx),
            EitherPin::Right(s2) => s2.poll_progress(cx),
        }
    }
}

pin_project! {
    #[must_use]
    #[project = MergeProj]
    pub struct Merge<Left, Right>
    where
        Left: AsyncIterator,
        Right: AsyncIterator<Item = Left::Item>,
    {
        #[pin]
        left: Fuse<Left>,
        #[pin]
        right: Fuse<Right>,
        poll_next_never_called: bool,
        left_is_current: bool,
        buffered: Option<Left::Item>,
    }
}

impl<'p, Left, Right> MergeProj<'p, Left, Right>
where
    Left: AsyncIterator,
    Right: AsyncIterator<Item = Left::Item>,
{
    fn current_iter(
        self: &mut MergeProj<'p, Left, Right>,
    ) -> EitherPin<'_, Fuse<Left>, Fuse<Right>> {
        if *self.left_is_current {
            EitherPin::Left(self.left.as_mut())
        } else {
            EitherPin::Right(self.right.as_mut())
        }
    }

    fn other_iter(self: &mut MergeProj<'p, Left, Right>) -> EitherPin<'_, Fuse<Left>, Fuse<Right>> {
        if *self.left_is_current {
            EitherPin::Right(self.right.as_mut())
        } else {
            EitherPin::Left(self.left.as_mut())
        }
    }

    fn swap_current_iter(self: &mut MergeProj<'p, Left, Right>) {
        *self.left_is_current = !*self.left_is_current;
    }
}

impl<Left, Right> AsyncIterator for Merge<Left, Right>
where
    Left: AsyncIterator,
    Right: AsyncIterator<Item = Left::Item>,
{
    type Item = Left::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollNext<Self::Item> {
        let mut this = self.project();
        *this.poll_next_never_called = false;
        if let Some(item) = this.buffered.take() {
            // If there's a buffered item, `poll_progress` got it from the current iterator.
            this.swap_current_iter();
            return PollNext::Item(item);
        }
        if let PollNext::Item(item) = this.current_iter().poll_next(cx) {
            this.swap_current_iter();
            return PollNext::Item(item);
        }
        if let PollNext::Item(item) = this.other_iter().poll_next(cx) {
            return PollNext::Item(item);
        }
        if this.left.is_done() && this.right.is_done() {
            PollNext::Done
        } else {
            PollNext::Pending
        }
    }

    fn poll_progress(self: Pin<&mut Self>, cx: &mut Context) -> Poll<()> {
        let mut this = self.project();
        if *this.poll_next_never_called {
            let poll1 = this.left.as_mut().poll_progress(cx);
            let poll2 = this.right.as_mut().poll_progress(cx);
            return any_pending([poll1, poll2]);
        }
        let mut ret = Poll::Ready(());
        if this.buffered.is_none() {
            match this.current_iter().poll_next(cx) {
                PollNext::Item(item) => *this.buffered = Some(item),
                PollNext::Done => {}
                PollNext::Pending => ret = Poll::Pending,
            }
        }
        if this.buffered.is_some() {
            let poll = this.current_iter().poll_progress(cx);
            update_pending(&mut ret, poll);
        }
        let poll = this.other_iter().poll_progress(cx);
        update_pending(&mut ret, poll);
        ret
    }
}

pin_project! {
    #[must_use]
    struct ForEach<Iter, F, Fut>
    where
        Iter: AsyncIterator,
        F: FnMut(Iter::Item) -> Fut,
        Fut: Future,
    {
        #[pin]
        iter: Iter,
        f: F,
        #[pin]
        fut: Option<Fut>,
        progress_pending: bool,
    }
}

impl<Iter, F, Fut> Future for ForEach<Iter, F, Fut>
where
    Iter: AsyncIterator,
    F: FnMut(Iter::Item) -> Fut,
    Fut: Future<Output = ()>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let mut this = self.project();
        loop {
            // If we need a new future, try to get one. If we can't get one, short-circuit.
            if this.fut.is_none() {
                match this.iter.as_mut().poll_next(cx) {
                    PollNext::Item(item) => {
                        let fut = (this.f)(item);
                        this.fut.set(Some(fut));
                        *this.progress_pending = true;
                        // If the new future is ready on its first poll below, we'll loop around
                        // and try to make another one. If not, we'll poll_progress before we
                        // yield.
                    }
                    PollNext::Done => return Poll::Ready(()),
                    PollNext::Pending => return Poll::Pending,
                }
            }
            // We have a future. Try to finish it.
            match this.fut.as_mut().as_pin_mut().unwrap().poll(cx) {
                Poll::Ready(()) => {
                    this.fut.set(None);
                    // Loop around and try to get another future.
                }
                Poll::Pending => {
                    // If the future is pending, let the iterator make progress concurrently.
                    if *this.progress_pending {
                        *this.progress_pending = this.iter.as_mut().poll_progress(cx).is_pending();
                    }
                    return Poll::Pending;
                }
            }
        }
    }
}

pin_project! {
    #[must_use]
    struct Once<Fut: Future> {
        #[pin]
        future: Option<Fut>,
        output: Option<Fut::Output>,
    }
}

fn once<Fut: Future>(future: Fut) -> Once<Fut> {
    Once {
        future: Some(future),
        output: None,
    }
}

impl<Fut: Future> AsyncIterator for Once<Fut> {
    type Item = Fut::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollNext<Self::Item> {
        let mut this = self.project();
        if let Some(output) = this.output.take() {
            PollNext::Item(output)
        } else if let Some(future) = this.future.as_mut().as_pin_mut() {
            match future.poll(cx) {
                Poll::Ready(output) => {
                    this.future.set(None);
                    PollNext::Item(output)
                }
                Poll::Pending => PollNext::Pending,
            }
        } else {
            PollNext::Done
        }
    }

    fn poll_progress(self: Pin<&mut Self>, cx: &mut Context) -> Poll<()> {
        let mut this = self.project();
        // It should be rare for `Once::poll_progress` to be called while `future` is `Some`, but
        // it can happen for example on the right side of a `Chain`. This is different from `Then`,
        // which has no future to drive in the same situation. When the `AsyncFn` traits are better
        // supported in the (hopefully near) future, it would be interesting to consider a variant
        // of `Once` that took an `AsyncFnOnce` rather than a `Future`. That version could have a
        // no-op `poll_progress`.
        if let Some(future) = this.future.as_mut().as_pin_mut() {
            match future.poll(cx) {
                Poll::Ready(output) => {
                    *this.output = Some(output);
                    this.future.set(None);
                    Poll::Ready(())
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Ready(())
        }
    }
}

struct Lock<T> {
    value: std::sync::Mutex<T>,
    wakers: StdMutex<Vec<Waker>>,
}

impl<T> Lock<T> {
    const fn new(value: T) -> Self {
        Self {
            value: StdMutex::new(value),
            wakers: StdMutex::new(Vec::new()),
        }
    }

    async fn lock(&self) -> Guard<'_, T> {
        poll_fn(|cx| {
            let mut wakers = self.wakers.lock().unwrap();
            match self.value.try_lock() {
                Ok(guard) => Poll::Ready(Guard::new(self, guard)),
                Err(TryLockError::Poisoned(_)) => panic!("poisoned"),
                Err(TryLockError::WouldBlock) => {
                    wakers.push(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
        .await
    }
}

struct Guard<'a, T> {
    mutex: &'a Lock<T>,
    std_guard: StdMutexGuard<'a, T>,
}

impl<'a, T> Guard<'a, T> {
    fn new(mutex: &'a Lock<T>, std_guard: StdMutexGuard<'a, T>) -> Self {
        Self { mutex, std_guard }
    }
}

impl<T> Deref for Guard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.std_guard
    }
}

impl<T> DerefMut for Guard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.std_guard
    }
}

impl<T> Drop for Guard<'_, T> {
    fn drop(&mut self) {
        // XXX: This is a hacky implementation, and there are multithreading bugs here. (This
        // example is single-task/single-thread, and the only reason we use `std::sync::Mutex` at
        // all here is to be `Sync` for the `static` below.) Another thread could respond to `wake`
        // and call `try_lock` before we return and drop the guard, which would cause a missed
        // wakeup. A custom `Waker` implementation could also try to lock `wakers` reentrantly,
        // which would deadlock.
        self.mutex
            .wakers
            .lock()
            .unwrap()
            .drain(..)
            .for_each(Waker::wake);
    }
}

// `do_work` takes a private lock, sleeps briefly, and releases it. A deadlock here shouldn't be
// possible.
async fn do_work() {
    static LOCK: Lock<()> = Lock::new(());
    let guard = LOCK.lock().await;
    sleep(Duration::from_millis(10)).await;
    // NOTE: The original `Merge` deadlock happens because the "losing" side of the `Merge` holds a
    // place in the waiters queue. But the hacky `Lock` implementation above isn't "fair" and
    // doesn't manage a queue. To make it possible for this version to deadlock -- for example if
    // you put an early return in `Merge::poll_progress` or comment out the call to `poll_progress`
    // in `ForEach::poll` -- we need to add an extra sleep here. This gives the losing side a
    // chance to actually acquire the lock, before the `ForEach` body starts.
    drop(guard);
    sleep(Duration::from_millis(5)).await;
}

#[tokio::main]
async fn main() {
    let my_iter = once(do_work()).merge(once(do_work()));
    my_iter
        .for_each(|_| async {
            println!("We make it here...");
            do_work().await;
            println!("...and here too!");
        })
        .await;
}
