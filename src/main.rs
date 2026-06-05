use pin_project_lite::pin_project;
use std::future::Future;
use std::mem;
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
            stream2: other.fuse(),
            progress: MergeProgress::Invalid,
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
    Once {
        future: Some(future),
    }
}

pin_project! {
    #[must_use]
    struct Once<Fut>
    where
        Fut: Future,
    {
        #[pin]
        future: Option<Fut>,
    }
}

impl<Fut: Future> AsyncIterator for Once<Fut> {
    type Item = Fut::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match this.future.as_mut().as_pin_mut() {
            Some(fut) => match fut.poll(cx) {
                Poll::Ready(output) => {
                    this.future.set(None);
                    Poll::Ready(Some(output))
                }
                Poll::Pending => Poll::Pending,
            },
            None => Poll::Ready(None),
        }
    }

    fn poll_progress(self: Pin<&mut Self>, _: &mut Context) -> Poll<()> {
        match &self.future {
            Some(_) => panic!("poll_progress called before poll_next"),
            None => Poll::Ready(()),
        }
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
        match this.stream.as_mut().as_pin_mut() {
            Some(stream) => match stream.poll_next(cx) {
                Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
                Poll::Ready(None) => {
                    this.stream.set(None);
                    Poll::Ready(None)
                }
                Poll::Pending => Poll::Pending,
            },
            None => Poll::Ready(None),
        }
    }

    fn poll_progress(self: Pin<&mut Self>, cx: &mut Context) -> Poll<()> {
        let this = self.project();
        match this.stream.as_pin_mut() {
            Some(stream) => stream.poll_progress(cx),
            None => Poll::Ready(()),
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

        let fut = this.fut.as_mut().as_pin_mut().expect("populated above");
        match fut.poll(cx) {
            Poll::Ready(output) => {
                this.fut.set(None);
                Poll::Ready(Some(output))
            }
            Poll::Pending => {
                _ = this.stream.as_mut().poll_progress(cx);
                Poll::Pending
            }
        }
    }

    fn poll_progress(self: Pin<&mut Self>, cx: &mut Context) -> Poll<()> {
        let this = self.project();
        assert!(
            this.fut.is_none(),
            "can't call poll_progress while poll_next is pending"
        );
        this.stream.poll_progress(cx)
    }
}

enum MergeProgress<Item> {
    Invalid,
    PollNextStream1,
    PollNextStream2,
    Buffered(Item),
    EmptyBuffer,
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
        #[pin]
        stream2: Fuse<S2>,
        progress: MergeProgress<S1::Item>,
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
        let progress = mem::replace(this.progress, MergeProgress::Invalid);
        if let MergeProgress::Buffered(item) = progress {
            *this.progress = MergeProgress::EmptyBuffer;
            return Poll::Ready(Some(item));
        }
        // TODO: Some sort of fairness here.
        if let Poll::Ready(Some(item)) = this.stream1.as_mut().poll_next(cx) {
            *this.progress = MergeProgress::PollNextStream2;
            return Poll::Ready(Some(item));
        }
        if let Poll::Ready(Some(item)) = this.stream2.as_mut().poll_next(cx) {
            *this.progress = MergeProgress::PollNextStream1;
            return Poll::Ready(Some(item));
        }
        // It's not allowed to call poll_progress after poll_next returns Pending or Ready(None),
        // so leave the MergeProgress::Invalid in place.
        if this.stream1.is_done() && this.stream2.is_done() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    fn poll_progress(self: Pin<&mut Self>, cx: &mut Context) -> Poll<()> {
        let this = self.project();
        let mut pending = false;
        match this.progress {
            MergeProgress::Invalid => panic!("invalid call to poll_progress"),
            MergeProgress::Buffered(_) | MergeProgress::EmptyBuffer => {
                if this.stream1.poll_progress(cx).is_pending() {
                    pending = true;
                }
                if this.stream2.poll_progress(cx).is_pending() {
                    pending = true;
                }
            }
            MergeProgress::PollNextStream1 => {
                match this.stream1.poll_next(cx) {
                    Poll::Ready(Some(item)) => *this.progress = MergeProgress::Buffered(item),
                    Poll::Ready(None) => {}
                    Poll::Pending => pending = true,
                }
                if this.stream2.poll_progress(cx).is_pending() {
                    pending = true;
                }
            }
            MergeProgress::PollNextStream2 => {
                if this.stream1.poll_progress(cx).is_pending() {
                    pending = true;
                }
                match this.stream2.poll_next(cx) {
                    Poll::Ready(Some(item)) => *this.progress = MergeProgress::Buffered(item),
                    Poll::Ready(None) => {}
                    Poll::Pending => pending = true,
                }
            }
        }
        if pending {
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
            // If we need a new future, try to get one. If we can't get one, short-circuit.
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

            // We have a future. Try to finish it.
            if this.fut.as_mut().as_pin_mut().unwrap().poll(cx).is_ready() {
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

    let start = Instant::now();
    iter(0..10)
        .then(async |x| {
            sleep(Duration::from_millis(100)).await;
            x
        })
        .for_each(async |x| {
            sleep(Duration::from_millis(100)).await;
            let elapsed = Instant::elapsed(&start).as_secs_f32();
            println!("[{elapsed:.3}s] {x}");
        })
        .await;

    println!("-----");

    async fn do_work(x: u64) -> u64 {
        sleep(Duration::from_millis(10)).await;
        x + 1
    }

    let start = Instant::now();
    iter(0..10)
        .then(do_work)
        .then(do_work)
        .then(do_work)
        .then(do_work)
        .then(do_work)
        .then(do_work)
        .then(do_work)
        .then(do_work)
        .then(do_work)
        .then(do_work)
        .for_each(async |x| {
            let elapsed = Instant::elapsed(&start).as_secs_f32();
            println!("[{elapsed:.3}s] {x}");
        })
        .await;
}
