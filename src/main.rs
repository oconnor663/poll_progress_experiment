use futures::future::MaybeDone;
use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};

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
            output: None,
        }
    }

    fn merge<S2>(self, other: S2) -> Merge<Self, S2>
    where
        Self: Sized,
    {
        Merge {
            stream1: Some(self),
            stream2: Some(other),
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
    Once {
        maybe_done: MaybeDone::Future(future),
    }
}

pin_project! {
    #[must_use]
    struct Once<Fut>
    where
        Fut: Future,
    {
        #[pin]
        maybe_done: MaybeDone<Fut>,
    }
}

impl<Fut: Future> AsyncIterator for Once<Fut> {
    type Item = Fut::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if let MaybeDone::Gone = &*this.maybe_done {
            return Poll::Ready(None);
        }
        _ = this.maybe_done.as_mut().poll(cx);
        if let Some(output) = this.maybe_done.take_output() {
            Poll::Ready(Some(output))
        } else {
            Poll::Pending
        }
    }

    fn poll_progress(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.project();
        if let MaybeDone::Gone = &*this.maybe_done {
            return Poll::Ready(());
        }
        this.maybe_done.poll(cx)
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
        output: Option<Fut::Output>,
    }
}

impl<S, F, Fut> AsyncIterator for Then<S, F, Fut>
where
    S: AsyncIterator,
    F: FnMut(S::Item) -> Fut,
    Fut: Future,
{
    type Item = Fut::Output;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Fut::Output>> {
        _ = self.as_mut().poll_progress(cx);
        let this = self.project();
        if let Some(output) = this.output.take() {
            Poll::Ready(Some(output))
        } else if this.stream.is_none() && this.fut.is_none() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    fn poll_progress(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut this = self.project();
        debug_assert!(this.fut.is_none() || this.output.is_none());
        let mut is_pending = false;

        if let Some(stream) = this.stream.as_mut().as_pin_mut() {
            if this.fut.is_none() && this.output.is_none() {
                match stream.poll_next(cx) {
                    Poll::Ready(Some(item)) => {
                        let fut = (this.f)(item);
                        this.fut.set(Some(fut));
                    }
                    Poll::Ready(None) => {
                        this.stream.set(None);
                        return Poll::Ready(());
                    }
                    Poll::Pending => return Poll::Pending,
                }
            } else if stream.poll_progress(cx).is_pending() {
                is_pending = true;
            }
        }

        if let Some(fut) = this.fut.as_mut().as_pin_mut() {
            debug_assert!(this.output.is_none());
            match fut.poll(cx) {
                Poll::Ready(output) => {
                    this.fut.set(None);
                    *this.output = Some(output);
                }
                Poll::Pending => is_pending = true,
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
    struct Merge<S1, S2> {
        #[pin]
        stream1: Option<S1>,
        #[pin]
        stream2: Option<S2>,
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
        let this = self.project();
        let poll1 = this.stream1.as_pin_mut().map(|s| s.poll_progress(cx));
        let poll2 = this.stream2.as_pin_mut().map(|s| s.poll_progress(cx));
        if matches!(poll1, Some(Poll::Pending)) || matches!(poll2, Some(Poll::Pending)) {
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
            if let Some(stream) = this.stream.as_mut().as_pin_mut() {
                if this.fut.is_none() {
                    match stream.poll_next(cx) {
                        Poll::Ready(Some(item)) => {
                            let fut = (this.f)(item);
                            this.fut.set(Some(fut));
                            // If the new future is ready on its first poll below, we'll loop
                            // around and try to make another one. If not, we'll loop around and
                            // poll_progress.
                        }
                        Poll::Ready(None) => {
                            this.stream.set(None);
                            return Poll::Ready(());
                        }
                        // `this.fut` is `None` here, so we short-circuit.
                        Poll::Pending => return Poll::Pending,
                    }
                } else {
                    _ = stream.poll_progress(cx);
                }
            }

            if let Some(fut) = this.fut.as_mut().as_pin_mut() {
                match fut.poll(cx) {
                    Poll::Ready(()) => {
                        this.fut.set(None);
                        if this.stream.is_none() {
                            return Poll::Ready(());
                        }
                        // Loop around. If the stream is still alive, we'll run another future.
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
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
}
