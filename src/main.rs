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
            iter: self,
            f,
            fut: None,
        }
    }

    fn map<F, T>(self, f: F) -> Map<Self, F>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> T,
    {
        Map { iter: self, f }
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
        }
    }
}

enum PollNext<Item> {
    Item(Item),
    Pending,
    Done,
}

impl<Item> PollNext<Item> {
    fn map<T>(self, f: impl FnOnce(Item) -> T) -> PollNext<T> {
        match self {
            PollNext::Item(item) => PollNext::Item(f(item)),
            PollNext::Pending => PollNext::Pending,
            PollNext::Done => PollNext::Done,
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
    struct Map<Iter, F> {
        #[pin]
        iter: Iter,
        f: F,
    }
}

impl<Iter, F, T> AsyncIterator for Map<Iter, F>
where
    Iter: AsyncIterator,
    F: FnMut(Iter::Item) -> T,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollNext<Self::Item> {
        let this = self.project();
        this.iter.poll_next(cx).map(this.f)
    }

    fn poll_progress(self: Pin<&mut Self>, cx: &mut Context) -> Poll<()> {
        self.project().iter.poll_progress(cx)
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
        iter: Iter,
        f: F,
        #[pin]
        fut: Option<Fut>,
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

        if this.fut.is_none() {
            match this.iter.as_mut().poll_next(cx) {
                PollNext::Item(item) => {
                    let fut = (this.f)(item);
                    this.fut.set(Some(fut));
                }
                PollNext::Done => return PollNext::Done,
                PollNext::Pending => return PollNext::Pending,
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
        assert!(self.fut.is_none());
        self.project().iter.poll_progress(cx)
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
                    _ = this.iter.as_mut().poll_progress(cx);
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
