use std::alloc::Layout;
use std::cell::Cell;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Waker;

use crate::raw::TaskVTable;
use crate::state::*;
use crate::utils::{abort_on_panic, extend};

/// The header of a task.
///
/// This header is stored right at the beginning of every heap-allocated task.
pub(crate) struct Header {
    /// Current state of the task.
    ///
    /// Contains flags representing the current state and the reference count.
    pub(crate) state: AtomicUsize,

    /// The task that is blocked on the `JoinHandle`.
    ///
    /// This waker needs to be woken up once the task completes or is closed.
    pub(crate) awaiter: Cell<Option<Waker>>,

    /// The virtual table.
    ///
    /// In addition to the actual waker virtual table, it also contains pointers to several other
    /// methods necessary for bookkeeping the heap-allocated task.
    pub(crate) vtable: &'static TaskVTable,
}

impl Header {
    /// Cancels the task.
    ///
    /// This method will mark the task as closed and notify the awaiter, but it won't reschedule
    /// the task if it's not completed.
    pub(crate) fn cancel(&self) {
        let mut state = self.state.load(Ordering::Acquire);

        loop {
            // If the task has been completed or closed, it can't be cancelled.
            if state & (COMPLETED | CLOSED) != 0 {
                break;
            }

            // Mark the task as closed.
            match self.state.compare_exchange_weak(
                state,
                state | CLOSED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // Notify the awaiter that the task has been closed.
                    if state & AWAITER != 0 {
                        self.notify(None);
                    }

                    break;
                }
                Err(s) => state = s,
            }
        }
    }

    /// Notifies the awaiter blocked on this task.
    ///
    /// If the awaiter is the same as the current waker, it will not be notified.
    #[inline]
    pub(crate) fn notify(&self, current: Option<&Waker>) {
        // We're about to try acquiring the lock in a loop. If it's already being held by another
        // thread, we'll have to spin for a while and yield the current thread.
        loop {
            // Acquire the lock. If we're storing an awaiter, then also set the awaiter flag.
            let state = self.state.fetch_or(LOCKED, Ordering::Acquire);

            // If the lock was acquired, break from the loop.
            if state & LOCKED == 0 {
                break;
            }

            // Yield because the lock is held by another thread.
            std::thread::yield_now();
        }

        // Replace the awaiter.
        let old = self.awaiter.take();

        // Release the lock. If we've cleared the awaiter, then also unset the awaiter flag.
        self.state.fetch_and(!LOCKED & !AWAITER, Ordering::Release);

        if let Some(w) = old {
            if let Some(c) = current {
                if w.will_wake(&c) {
                    return;
                }
            }

            // We need a safeguard against panics because waking can panic.
            abort_on_panic(|| w.wake());
        }
    }

    #[inline]
    pub(crate) fn register(&self, new: Waker) {
        // We're about to try acquiring the lock in a loop. If it's already being held by another
        // thread, we'll have to spin for a while and yield the current thread.
        while self.state.fetch_or(LOCKED | AWAITER, Ordering::Acquire) & LOCKED != 0 {}

        // Replace the awaiter.
        self.awaiter.set(Some(new));

        // Release the lock. If we've cleared the awaiter, then also unset the awaiter flag.
        self.state.fetch_and(!LOCKED, Ordering::Release);
    }

    /// Returns the offset at which the tag of type `T` is stored.
    #[inline]
    pub(crate) fn offset_tag<T>() -> usize {
        let layout_header = Layout::new::<Header>();
        let layout_t = Layout::new::<T>();
        let (_, offset_t) = extend(layout_header, layout_t);
        offset_t
    }
}

impl fmt::Debug for Header {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.load(Ordering::SeqCst);

        f.debug_struct("Header")
            .field("scheduled", &(state & SCHEDULED != 0))
            .field("running", &(state & RUNNING != 0))
            .field("completed", &(state & COMPLETED != 0))
            .field("closed", &(state & CLOSED != 0))
            .field("awaiter", &(state & AWAITER != 0))
            .field("handle", &(state & HANDLE != 0))
            .field("ref_count", &(state / REFERENCE))
            .finish()
    }
}
