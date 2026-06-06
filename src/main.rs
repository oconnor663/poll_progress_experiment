#![feature(async_iterator)]
#![feature(async_for_loop)]
#![feature(gen_blocks)]

use pin_project_lite::pin_project;
use std::async_iter::AsyncIterator;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};

fn merge<S1, S2>(stream1: S1, stream2: S2) -> Merge<S1, S2>
where
    S1: AsyncIterator,
    S2: AsyncIterator<Item = S1::Item>,
{
    Merge {
        stream1: Some(stream1),
        stream2: Some(stream2),
    }
}

pin_project! {
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

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
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
}

async gen fn foo() {
    println!("2. `foo` starts a sleep.");
    // If the main loop doesn't drive the `Merge` using `poll_progress`, we get stuck here and
    // deadlock the main loop.
    sleep(Duration::from_millis(20)).await;
    println!("4. `foo` finishes its sleep and yields to `bar`.");
    // If `Merge::poll_progress` doesn't call `poll_next` on `bar`, we get stuck here and deadlock
    // the main loop.
    yield;
}

static X: Mutex<()> = Mutex::const_new(());

async gen fn bar() {
    println!("1. `bar` locks `X` immediately and starts iterating over `foo`.");
    let guard = X.lock().await;
    for await () in foo() {}
    println!("5. `bar` finishes iterating over `foo` and unlocks `X`.");
    drop(guard);
    yield;
}

async gen fn fast() {
    sleep(Duration::from_millis(10)).await;
    yield;
}

#[tokio::main]
async fn main() {
    for await () in merge(bar(), fast()) {
        println!("3. The main loop tries to lock `X` and deadlocks without `poll_progress`.");
        // Deadlock! Currently `AsyncIterator` has no `poll_progress` method, so `foo` is snoozed
        // on line 67 while holding the `X` lock. However, when `poll_progress` is added, we *also*
        // need to ensure that control flows smoothly between lines 71 and 81, or else we'll still
        // deadlock here. That has implications for how `Merge` drives its children. See above.
        _ = *X.lock().await;
    }
}
