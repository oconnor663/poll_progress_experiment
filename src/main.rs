use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant, sleep};

trait AsyncIterator {
    type Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>>;

    fn poll_progress(self: Pin<&mut Self>, cx: &mut Context) -> Poll<()>;

    fn fuse(self) -> Fuse<Self>
    where
        Self: Sized,
    {
        Fuse { stream: Some(self) }
    }

    fn then<F, Fut>(self, f: F) -> Then<Self, F, Fut>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> Fut,
        Fut: Future,
    {
        Then {
            stream: self.fuse(),
            f,
            fut: None,
        }
    }

    fn merge<S2>(self, other: S2) -> Merge<Self, S2>
    where
        Self: Sized,
        S2: AsyncIterator<Item = Self::Item>,
    {
        Merge {
            stream1: self.fuse(),
            item1: None,
            stream2: other.fuse(),
            item2: None,
        }
    }

    fn for_each<F, Fut>(self, f: F) -> ForEach<Self, F, Fut>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> Fut,
        Fut: Future<Output = ()>,
    {
        ForEach {
            stream: self.fuse(),
            f,
            fut: None,
        }
    }
}

fn once<Fut>(future: Fut) -> Once<Fut>
where
    Fut: Future,
{
    Once { fut: Some(future) }
}

pin_project! {
    #[must_use]
    struct Once<Fut>
    where
        Fut: Future,
    {
        #[pin]
        fut: Option<Fut>,
    }
}

impl<Fut: Future> AsyncIterator for Once<Fut> {
    type Item = Fut::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if let Some(fut) = this.fut.as_mut().as_pin_mut() {
            match fut.poll(cx) {
                Poll::Ready(output) => {
                    this.fut.set(None);
                    Poll::Ready(Some(output))
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Ready(None)
        }
    }

    fn poll_progress(self: Pin<&mut Self>, _: &mut Context) -> Poll<()> {
        assert!(self.fut.is_none(), "not allowed!?");
        Poll::Ready(())
    }
}

fn iter<I>(iter: I) -> Iter<I::IntoIter>
where
    I: IntoIterator,
{
    Iter {
        iter: iter.into_iter(),
    }
}

struct Iter<I> {
    iter: I,
}

impl<I> Unpin for Iter<I> {}

impl<I> AsyncIterator for Iter<I>
where
    I: Iterator,
{
    type Item = I::Item;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.iter.next())
    }

    fn poll_progress(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<()> {
        Poll::Ready(())
    }
}

pin_project! {
    #[must_use]
    struct Fuse<S> {
        #[pin]
        stream: Option<S>,
    }
}

impl<S> Fuse<S> {
    fn is_done(&self) -> bool {
        self.stream.is_none()
    }
}

impl<S> AsyncIterator for Fuse<S>
where
    S: AsyncIterator,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<S::Item>> {
        let mut this = self.project();
        if let Some(stream) = this.stream.as_mut().as_pin_mut() {
            let poll = stream.poll_next(cx);
            if let Poll::Ready(None) = poll {
                this.stream.set(None);
            }
            poll
        } else {
            Poll::Ready(None)
        }
    }

    fn poll_progress(self: Pin<&mut Self>, cx: &mut Context) -> Poll<()> {
        let this = self.project();
        if let Some(stream) = this.stream.as_pin_mut() {
            stream.poll_progress(cx)
        } else {
            Poll::Ready(())
        }
    }
}

pin_project! {
    #[must_use]
    struct Then<S, F, Fut>
    where
        S: AsyncIterator,
        F: FnMut(S::Item) -> Fut,
        Fut: Future,
    {
        #[pin]
        stream: Fuse<S>,
        f: F,
        #[pin]
        fut: Option<Fut>,
    }
}

impl<S, F, Fut> AsyncIterator for Then<S, F, Fut>
where
    S: AsyncIterator,
    F: FnMut(S::Item) -> Fut,
    Fut: Future,
{
    type Item = Fut::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Fut::Output>> {
        let mut this = self.project();

        if this.fut.is_none() {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    let fut = (this.f)(item);
                    this.fut.set(Some(fut));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }

        match this.fut.as_mut().as_pin_mut().unwrap().poll(cx) {
            Poll::Ready(output) => {
                this.fut.set(None);
                Poll::Ready(Some(output))
                // We don't need `poll_progress` in this branch. The caller will call `poll_next`
                // or `poll_progress` again.
            }
            Poll::Pending => {
                _ = this.stream.poll_progress(cx);
                Poll::Pending
            }
        }
    }

    fn poll_progress(self: Pin<&mut Self>, cx: &mut Context) -> Poll<()> {
        let this = self.project();
        assert!(this.fut.is_none(), "not allowed!?");
        this.stream.poll_progress(cx)
    }
}

pin_project! {
    #[must_use]
    struct Merge<S1, S2>
    where
        S1: AsyncIterator,
        S2: AsyncIterator<Item = S1::Item>,
    {
        #[pin]
        stream1: Fuse<S1>,
        item1: Option<S1::Item>,
        #[pin]
        stream2: Fuse<S2>,
        item2: Option<S1::Item>,
    }
}

impl<S1, S2> AsyncIterator for Merge<S1, S2>
where
    S1: AsyncIterator,
    S2: AsyncIterator<Item = S1::Item>,
{
    type Item = S1::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if let Some(item) = this.item1.take() {
            return Poll::Ready(Some(item));
        }
        if let Some(item) = this.item2.take() {
            return Poll::Ready(Some(item));
        }
        if let Poll::Ready(Some(item)) = this.stream1.as_mut().poll_next(cx) {
            return Poll::Ready(Some(item));
        }
        if let Poll::Ready(Some(item)) = this.stream2.as_mut().poll_next(cx) {
            return Poll::Ready(Some(item));
        }
        if this.stream1.is_done() && this.stream2.is_done() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    fn poll_progress(self: Pin<&mut Self>, cx: &mut Context) -> Poll<()> {
        let mut this = self.project();
        let mut pending1 = false;
        if this.item1.is_none() {
            match this.stream1.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => *this.item1 = Some(item),
                Poll::Ready(None) => {}
                Poll::Pending => pending1 = true,
            }
        }
        if !pending1 {
            pending1 = this.stream1.poll_progress(cx).is_pending();
        }
        let mut pending2 = false;
        if this.item2.is_none() {
            match this.stream2.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => *this.item2 = Some(item),
                Poll::Ready(None) => {}
                Poll::Pending => pending2 = true,
            }
        }
        if !pending2 {
            pending2 = this.stream2.poll_progress(cx).is_pending();
        }
        if pending1 || pending2 {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

pin_project! {
    #[must_use]
    struct ForEach<S, F, Fut>
    where
        S: AsyncIterator,
        F: FnMut(S::Item) -> Fut,
        Fut: Future<Output = ()>,
    {
        #[pin]
        stream: Fuse<S>,
        f: F,
        #[pin]
        fut: Option<Fut>,
    }
}

impl<S, F, Fut> Future for ForEach<S, F, Fut>
where
    S: AsyncIterator,
    F: FnMut(S::Item) -> Fut,
    Fut: Future<Output = ()>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<()> {
        let mut this = self.project();
        loop {
            // If we need a new future, try to get one.
            if this.fut.is_none() {
                match this.stream.as_mut().poll_next(cx) {
                    Poll::Ready(Some(item)) => {
                        let fut = (this.f)(item);
                        this.fut.set(Some(fut));
                        // If the new future is ready on its first poll below, we'll loop around
                        // and try to make another one. If not, we'll poll_progress before we
                        // yield.
                    }
                    Poll::Ready(None) => return Poll::Ready(()),
                    Poll::Pending => return Poll::Pending,
                }
            }

            // If we have a future, try to finish it.
            if let Some(fut) = this.fut.as_mut().as_pin_mut() {
                if fut.poll(cx).is_ready() {
                    this.fut.set(None);
                    if this.stream.is_done() {
                        return Poll::Ready(());
                    } else {
                        // Loop around and try to get another future.
                        continue;
                    }
                } else {
                    // If the future is pending, let the stream make progress concurrently.
                    _ = this.stream.as_mut().poll_progress(cx);
                }
            }

            debug_assert!(this.fut.is_some() || !this.stream.is_done());
            return Poll::Pending;
        }
    }
}

async fn foo(i: i32) -> i32 {
    static LOCK: Mutex<()> = Mutex::const_new(());
    println!("foo({i})");
    let _guard = LOCK.lock().await;
    sleep(Duration::from_millis(10)).await;
    i
}

#[tokio::main]
async fn main() {
    let stream1 = once(foo(1));
    let stream2 = once(foo(2));
    let merged = stream1.merge(stream2);
    merged
        .then(async |i| foo(10 * i).await)
        .for_each(async |item| {
            println!("Got {:?}, calling foo(999)...", item);
            foo(999).await;
            println!("...foo(999) finished");
        })
        .await;

    println!("-----");

    let start_time = Instant::now();
    iter(0..10)
        .then(async |x| {
            sleep(Duration::from_millis(100)).await;
            x
        })
        .for_each(async |x| {
            sleep(Duration::from_millis(100)).await;
            let elapsed = Instant::elapsed(&start_time).as_secs_f32();
            println!("[{elapsed:.3}s] {x}");
        })
        .await;
}
