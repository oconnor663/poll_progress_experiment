use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::time::{Duration, sleep};

trait AsyncIterator {
    type Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollNext<Self::Item>;

    fn poll_progress(self: Pin<&mut Self>, cx: &mut Context) -> Poll<()>;

    fn fuse(self) -> Fuse<Self>
    where
        Self: Sized,
    {
        Fuse { iter: Some(self) }
    }

    fn then<F, Fut>(self, f: F) -> Then<Self, F, Fut>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> Fut,
        Fut: Future,
    {
        Then {
            iter: self.fuse(),
            next_item_wanted: false,
            f,
            fut: None,
            item: None,
        }
    }

    fn map<F, T>(self, f: F) -> Map<Self, F, T>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> T,
    {
        Map {
            iter: self.fuse(),
            next_item_wanted: false,
            f,
            item: None,
        }
    }

    fn merge<Other>(self, other: Other) -> Merge<Self, Other>
    where
        Self: Sized,
        Other: AsyncIterator<Item = Self::Item>,
    {
        Merge {
            left: self.fuse(),
            right: other.fuse(),
        }
    }

    fn try_for_each<F, Fut>(self, f: F) -> TryForEach<Self, F, Fut>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> Fut,
        Fut: Future<Output = ControlFlow>,
    {
        TryForEach {
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

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context) -> PollNext<Self::Item> {
        match self.iter.next() {
            Some(item) => PollNext::Item(item),
            None => PollNext::Done,
        }
    }

    fn poll_progress(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<()> {
        Poll::Ready(())
    }
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

pin_project! {
    #[must_use]
    struct Map<Iter, F, T> {
        #[pin]
        iter: Fuse<Iter>,
        next_item_wanted: bool,
        f: F,
        item: Option<T>,
    }
}

impl<Iter, F, T> AsyncIterator for Map<Iter, F, T>
where
    Iter: AsyncIterator,
    F: FnMut(Iter::Item) -> T,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollNext<Self::Item> {
        let mut this = self.project();
        if let Some(item) = this.item.take() {
            return PollNext::Item(item);
        }
        match this.iter.as_mut().poll_next(cx) {
            PollNext::Item(item) => {
                *this.next_item_wanted = false;
                PollNext::Item((this.f)(item))
            }
            PollNext::Pending => {
                *this.next_item_wanted = true;
                PollNext::Pending
            }
            PollNext::Done => PollNext::Done,
        }
    }

    fn poll_progress(self: Pin<&mut Self>, cx: &mut Context) -> Poll<()> {
        let mut this = self.project();
        if this.item.is_none() && *this.next_item_wanted {
            match this.iter.as_mut().poll_next(cx) {
                PollNext::Item(item) => {
                    *this.item = Some((this.f)(item));
                    *this.next_item_wanted = false;
                    this.iter.poll_progress(cx)
                }
                PollNext::Pending => Poll::Pending,
                PollNext::Done => Poll::Ready(()),
            }
        } else {
            this.iter.poll_progress(cx)
        }
    }
}

pin_project! {
    #[must_use]
    struct Then<Iter, F, Fut>
    where
        Iter: AsyncIterator,
        F: FnMut(Iter::Item) -> Fut,
        Fut: Future,
    {
        #[pin]
        iter: Fuse<Iter>,
        next_item_wanted: bool,
        f: F,
        #[pin]
        fut: Option<Fut>,
        item: Option<Fut::Output>,
    }
}

impl<Iter, F, Fut> AsyncIterator for Then<Iter, F, Fut>
where
    Iter: AsyncIterator,
    F: FnMut(Iter::Item) -> Fut,
    Fut: Future,
{
    type Item = Fut::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollNext<Fut::Output> {
        let mut this = self.project();
        if let Some(item) = this.item.take() {
            return PollNext::Item(item);
        }
        if this.fut.is_none() {
            match this.iter.as_mut().poll_next(cx) {
                PollNext::Item(item) => {
                    *this.next_item_wanted = false;
                    let fut = (this.f)(item);
                    this.fut.set(Some(fut));
                }
                PollNext::Pending => {
                    *this.next_item_wanted = true;
                    return PollNext::Pending;
                }
                PollNext::Done => return PollNext::Done,
            }
        }
        let fut = this.fut.as_mut().as_pin_mut().expect("populated above");
        match fut.poll(cx) {
            Poll::Ready(output) => {
                this.fut.set(None);
                PollNext::Item(output)
            }
            Poll::Pending => {
                _ = this.iter.as_mut().poll_progress(cx);
                PollNext::Pending
            }
        }
    }

    fn poll_progress(self: Pin<&mut Self>, cx: &mut Context) -> Poll<()> {
        let mut this = self.project();
        if this.item.is_some() {
            return this.iter.poll_progress(cx);
        }
        if this.fut.is_none() && *this.next_item_wanted {
            match this.iter.as_mut().poll_next(cx) {
                PollNext::Item(item) => {
                    *this.next_item_wanted = false;
                    this.fut.set(Some((this.f)(item)));
                }
                PollNext::Pending => return Poll::Pending,
                PollNext::Done => return Poll::Ready(()),
            }
        }
        if let Some(fut) = this.fut.as_mut().as_pin_mut() {
            match fut.poll(cx) {
                Poll::Ready(item) => {
                    this.fut.set(None);
                    *this.item = Some(item);
                    this.iter.poll_progress(cx)
                }
                Poll::Pending => {
                    _ = this.iter.poll_progress(cx);
                    Poll::Pending
                }
            }
        } else {
            this.iter.poll_progress(cx)
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

pin_project! {
    #[must_use]
    pub struct Merge<Left, Right>
    where
        Left: AsyncIterator,
        Right: AsyncIterator<Item = Left::Item>,
    {
        #[pin]
        left: Fuse<Left>,
        #[pin]
        right: Fuse<Right>,
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
        // XXX: This implementation is simple but unfair. If the left is always ready, the right
        // will never get polled. Not a problem in this particular example.
        if let PollNext::Item(item) = this.left.as_mut().poll_next(cx) {
            return PollNext::Item(item);
        }
        if let PollNext::Item(item) = this.right.as_mut().poll_next(cx) {
            return PollNext::Item(item);
        }
        if this.left.is_done() && this.right.is_done() {
            PollNext::Done
        } else {
            PollNext::Pending
        }
    }

    fn poll_progress(self: Pin<&mut Self>, cx: &mut Context) -> Poll<()> {
        let this = self.project();
        // XXX: This implementation is lazy. It makes its callees much more complicated.
        let poll1 = this.left.poll_progress(cx);
        let poll2 = this.right.poll_progress(cx);
        any_pending([poll1, poll2])
    }
}

pub enum ControlFlow {
    Continue,
    Break,
}

pin_project! {
    #[must_use]
    struct TryForEach<Iter, F, Fut>
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

impl<Iter, F, Fut> Future for TryForEach<Iter, F, Fut>
where
    Iter: AsyncIterator,
    F: FnMut(Iter::Item) -> Fut,
    Fut: Future<Output = ControlFlow>,
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
                Poll::Ready(output) => match output {
                    ControlFlow::Continue => {
                        this.fut.set(None);
                        // Loop around and try to get another future.
                    }
                    ControlFlow::Break => {
                        return Poll::Ready(());
                    }
                },
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

// async gen fn slow_numbers() -> u32 {
//     for i in 0..10 {
//         sleep(Duration::from_millis(1)).await;
//         yield i;
//     }
// }
fn slow_numbers() -> impl AsyncIterator<Item = u32> {
    iter(0..10).then(async |i| {
        sleep(Duration::from_millis(1)).await;
        i
    })
}

fn print_numbers() -> impl AsyncIterator<Item = u32> {
    slow_numbers().map(|i| {
        println!("NUMBER {i}");
        i
    })
}

#[tokio::main]
async fn main() {
    println!("--- first loop ---");
    // for await _ in print_numbers() {
    //     sleep(Duration::from_millis(10)).await;
    //     break;
    // }
    print_numbers()
        .try_for_each(async |_| {
            sleep(Duration::from_millis(10)).await;
            ControlFlow::Break
        })
        .await;

    println!("\n---second loop ---");
    // for await _ in print_numbers().merge(print_numbers()) {
    //     sleep(Duration::from_millis(10)).await;
    //     break;
    // }
    print_numbers()
        .merge(print_numbers())
        .try_for_each(async |_| {
            sleep(Duration::from_millis(10)).await;
            ControlFlow::Break
        })
        .await;
}
