use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant, sleep};

trait AsyncIterator {
    type Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>;

    fn poll_progress(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()>;

    fn then<F, Fut>(self, f: F) -> Then<Self, F, Fut>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> Fut,
        Fut: Future,
    {
        Then {
            stream: Some(self),
            f,
            fut: None,
        }
    }

    fn merge<S>(self, other: S) -> Merge<Self, S>
    where
        Self: Sized,
        S: AsyncIterator<Item = Self::Item>,
    {
        Merge {
            stream1: Some(self),
            item1: None,
            stream2: Some(other),
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
            stream: Some(self),
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

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let Some(fut) = this.fut.as_mut().as_pin_mut() else {
            return Poll::Ready(None);
        };
        match fut.poll(cx) {
            Poll::Ready(output) => {
                this.fut.set(None);
                Poll::Ready(Some(output))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_progress(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        // XXX: Caller must keep calling `poll_next`.
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

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.iter.next())
    }

    fn poll_progress(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        Poll::Ready(())
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
        stream: Option<S>,
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

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Fut::Output>> {
        let mut this = self.project();

        if this.stream.is_none() && this.fut.is_none() {
            return Poll::Ready(None);
        }

        if let Some(stream) = this.stream.as_mut().as_pin_mut() {
            if this.fut.is_none() {
                match stream.poll_next(cx) {
                    Poll::Ready(Some(item)) => {
                        let fut = (this.f)(item);
                        this.fut.set(Some(fut));
                    }
                    Poll::Ready(None) => {
                        this.stream.set(None);
                        return Poll::Ready(None);
                    }
                    Poll::Pending => return Poll::Pending,
                }
            } else {
                _ = stream.poll_progress(cx);
            }
        }

        if let Some(fut) = this.fut.as_mut().as_pin_mut() {
            match fut.poll(cx) {
                Poll::Ready(output) => {
                    this.fut.set(None);
                    Poll::Ready(Some(output))
                }
                Poll::Pending => Poll::Pending,
            }
        } else if this.stream.is_some() {
            Poll::Pending
        } else {
            Poll::Ready(None)
        }
    }

    fn poll_progress(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.project();
        // XXX: If `self.fut` is non-None, caller must keep calling `poll_next`.
        if let Some(stream) = this.stream.as_pin_mut() {
            stream.poll_progress(cx)
        } else {
            Poll::Ready(())
        }
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
        stream1: Option<S1>,
        item1: Option<S1::Item>,
        #[pin]
        stream2: Option<S2>,
        item2: Option<S2::Item>,
    }
}

impl<S1, S2> AsyncIterator for Merge<S1, S2>
where
    S1: AsyncIterator,
    S2: AsyncIterator<Item = S1::Item>,
{
    type Item = S1::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if let Some(item1) = this.item1.take() {
            return Poll::Ready(Some(item1));
        }
        if let Some(item2) = this.item2.take() {
            return Poll::Ready(Some(item2));
        }
        if let Some(stream1) = this.stream1.as_mut().as_pin_mut() {
            match stream1.poll_next(cx) {
                Poll::Ready(Some(item)) => return Poll::Ready(Some(item)),
                Poll::Ready(None) => this.stream1.set(None),
                Poll::Pending => {}
            }
        }
        if let Some(stream2) = this.stream2.as_mut().as_pin_mut() {
            match stream2.poll_next(cx) {
                Poll::Ready(Some(item)) => return Poll::Ready(Some(item)),
                Poll::Ready(None) => this.stream2.set(None),
                Poll::Pending => {}
            }
        }
        if this.stream1.is_none() && this.stream2.is_none() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    fn poll_progress(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut this = self.project();
        let mut is_pending = false;
        if let Some(stream1) = this.stream1.as_mut().as_pin_mut() {
            if this.item1.is_none() {
                match stream1.poll_next(cx) {
                    Poll::Ready(Some(item)) => *this.item1 = Some(item),
                    Poll::Ready(None) => this.stream1.set(None),
                    Poll::Pending => is_pending = true,
                }
            } else if stream1.poll_progress(cx).is_pending() {
                is_pending = true;
            }
        }
        if let Some(stream2) = this.stream2.as_mut().as_pin_mut() {
            if this.item2.is_none() {
                match stream2.poll_next(cx) {
                    Poll::Ready(Some(item)) => *this.item2 = Some(item),
                    Poll::Ready(None) => this.stream2.set(None),
                    Poll::Pending => is_pending = true,
                }
            } else if stream2.poll_progress(cx).is_pending() {
                is_pending = true;
            }
        }
        if is_pending {
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
        stream: Option<S>,
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

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut this = self.project();
        loop {
            // If we need a new future, try to get one.
            if this.fut.is_none()
                && let Some(stream) = this.stream.as_mut().as_pin_mut()
            {
                match stream.poll_next(cx) {
                    Poll::Ready(Some(item)) => {
                        let fut = (this.f)(item);
                        this.fut.set(Some(fut));
                        // If the new future is ready on its first poll below, we'll loop
                        // around and try to make another one. If not, we'll poll_progress before
                        // we yield.
                    }
                    Poll::Ready(None) => {
                        this.stream.set(None);
                        return Poll::Ready(());
                    }
                    // `this.fut` is `None` here, so short-circuit.
                    Poll::Pending => return Poll::Pending,
                }
            }

            // If we have a future, try to finish it.
            if let Some(fut) = this.fut.as_mut().as_pin_mut() {
                if fut.poll(cx).is_ready() {
                    this.fut.set(None);
                    if this.stream.is_none() {
                        return Poll::Ready(());
                    } else {
                        // Loop around and try to get another future.
                        continue;
                    }
                } else if let Some(stream) = this.stream.as_mut().as_pin_mut() {
                    // If the future is pending, let the stream make progress concurrently.
                    _ = stream.poll_progress(cx);
                }
            }

            debug_assert!(this.fut.is_some() || this.stream.is_some());
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
