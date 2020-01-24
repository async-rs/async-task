#![feature(test)]

extern crate test;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use crossbeam::sync::Parker;
use test::Bencher;

/// Runs a future to completion on the current thread.
fn block_on<F: Future>(future: F) -> F::Output {
    thread_local! {
        // Parker and waker associated with the current thread.
        static PARKER_WAKER: (Parker, Waker) = {
            let parker = Parker::new();
            let unparker = parker.unparker().clone();
            let waker = async_task::waker_fn(move || unparker.unpark());
            (parker, waker)
        };
    }

    // Pin the future on the stack.
    pin_utils::pin_mut!(future);

    PARKER_WAKER.with(|(parker, waker)| {
        // Create the task context.
        let cx = &mut Context::from_waker(&waker);

        // Keep polling the future until completion.
        loop {
            match future.as_mut().poll(cx) {
                Poll::Ready(output) => return output,
                Poll::Pending => parker.park(),
            }
        }
    })
}

#[bench]
fn custom_block_on_0_yields(b: &mut Bencher) {
    b.iter(|| block_on(Yields(0)));
}

#[bench]
fn futures_block_on_0_yields(b: &mut Bencher) {
    b.iter(|| futures::executor::block_on(Yields(0)));
}

#[bench]
fn custom_block_on_100_yields(b: &mut Bencher) {
    b.iter(|| block_on(Yields(100)));
}

#[bench]
fn futures_block_on_100_yields(b: &mut Bencher) {
    b.iter(|| futures::executor::block_on(Yields(100)));
}

struct Yields(u32);

impl Future for Yields {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.0 == 0 {
            Poll::Ready(())
        } else {
            self.0 -= 1;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}
