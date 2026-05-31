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

fn once<Fut>(future: Fut) -> Once<Fut> {
    Once {
        future: Some(future),
    }
}

pin_project! {
    #[must_use]
    struct Once<Fut> {
        #[pin]
        future: Option<Fut>,
    }
}

impl<Fut: Future> AsyncIterator for Once<Fut> {
    type Item = Fut::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if let Some(future) = this.future.as_mut().as_pin_mut() {
            match future.poll(cx) {
                Poll::Ready(output) => {
                    this.future.set(None);
                    Poll::Ready(Some(output))
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Ready(None)
        }
    }

    fn poll_progress(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        Poll::Ready(())
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
        let mut this = self.project();
        let poll1 = this
            .stream1
            .as_mut()
            .as_pin_mut()
            .map(|s| s.poll_progress(cx));
        let poll2 = this
            .stream2
            .as_mut()
            .as_pin_mut()
            .map(|s| s.poll_progress(cx));
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
                            if this.fut.is_none() {
                                return Poll::Ready(());
                            }
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
        .for_each(async |item| {
            println!("Got {:?}, calling foo(3)...", item);
            foo(3).await;
            println!("...foo(3) finished");
        })
        .await;
}
