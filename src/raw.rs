use core::cell::UnsafeCell;
use core::future::Future;
use core::hint;
use core::marker::PhantomData;
use core::mem::{self, ManuallyDrop};
use core::pin::Pin;
use core::ptr::{self, NonNull};
use core::sync::atomic::{AtomicUsize, Ordering};
use core::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use alloc::boxed::Box;

use crate::header::Header;
use crate::state::*;
use crate::utils::{abort, abort_on_panic};
use crate::Task;

/// The vtable for a task.
pub(crate) struct TaskVTable {
    /// Schedules the task.
    pub(crate) schedule: unsafe fn(*const ()),

    /// Drops the future inside the task.
    pub(crate) drop_future: unsafe fn(*const ()),

    /// Returns a pointer to the output stored after completion.
    pub(crate) get_output: unsafe fn(*const ()) -> *const (),

    /// Drops the task.
    pub(crate) drop_task: unsafe fn(ptr: *const ()),

    /// Destroys the task.
    pub(crate) destroy: unsafe fn(*const ()),

    /// Runs the task.
    pub(crate) run: unsafe fn(*const ()),

    /// Creates a new waker associated with the task.
    pub(crate) clone_waker: unsafe fn(ptr: *const ()) -> RawWaker,
}

/// The raw in-memory representation of a task.
#[repr(C)]
pub(crate) struct RawTask<F, R, S, T> {
    /// The task header.
    header: Header,
    /// The tag inside the task.
    tag: T,
    /// The schedule function.
    schedule: S,
    /// The future and, after its completion, the future's output.
    future_then_output: FutureThenOutput<F, R>,
}

/// Union containing first the future and then its output.
enum FutureThenOutput<F, R> {
    /// The future (initial variant).
    Future(F),
    /// The future's output (replaces the future after its dropped).
    Output(R),
}

impl<F, R> FutureThenOutput<F, R> {
    unsafe fn future(&mut self) -> *mut F {
        match self {
            FutureThenOutput::Future(future) => future,
            FutureThenOutput::Output(_) => hint::unreachable_unchecked(),
        }
    }

    unsafe fn output(&self) -> *const R {
        match self {
            FutureThenOutput::Output(output) => output,
            FutureThenOutput::Future(_) => hint::unreachable_unchecked(),
        }
    }
}

impl<F, R, S, T> RawTask<F, R, S, T>
where
    F: Future<Output = R> + 'static,
    S: Fn(Task<T>) + Send + Sync + 'static,
{
    const RAW_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
        Self::clone_waker,
        Self::wake,
        Self::wake_by_ref,
        Self::drop_waker,
    );

    /// Allocates a task with the given `future` and `schedule` function.
    ///
    /// It is assumed that initially only the `Task` reference and the `JoinHandle` exist.
    pub(crate) fn allocate(future: F, schedule: S, tag: T) -> NonNull<()> {
        let raw_task = Box::new(Self {
            header: Header {
                state: AtomicUsize::new(SCHEDULED | HANDLE | REFERENCE),
                awaiter: UnsafeCell::new(None),
                vtable: &TaskVTable {
                    schedule: Self::schedule,
                    drop_future: Self::drop_future,
                    get_output: Self::get_output,
                    drop_task: Self::drop_task,
                    destroy: Self::destroy,
                    run: Self::run,
                    clone_waker: Self::clone_waker,
                },
            },
            tag,
            schedule,
            future_then_output: FutureThenOutput::Future(future),
        });

        NonNull::from(Box::leak(raw_task)).cast()
    }

    /// Wakes a waker.
    unsafe fn wake(ptr: *const ()) {
        // This is just an optimization. If the schedule function has captured variables, then
        // we'll do less reference counting if we wake the waker by reference and then drop it.
        if mem::size_of::<S>() > 0 {
            Self::wake_by_ref(ptr);
            Self::drop_waker(ptr);
            return;
        }

        let raw = ptr as *const Self;

        let mut state = (*raw).header.state.load(Ordering::Acquire);

        loop {
            // If the task is completed or closed, it can't be woken up.
            if state & (COMPLETED | CLOSED) != 0 {
                // Drop the waker.
                Self::drop_waker(ptr);
                break;
            }

            // If the task is already scheduled, we just need to synchronize with the thread that
            // will run the task by "publishing" our current view of the memory.
            if state & SCHEDULED != 0 {
                // Update the state without actually modifying it.
                match (*raw).header.state.compare_exchange_weak(
                    state,
                    state,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        // Drop the waker.
                        Self::drop_waker(ptr);
                        break;
                    }
                    Err(s) => state = s,
                }
            } else {
                // Mark the task as scheduled.
                match (*raw).header.state.compare_exchange_weak(
                    state,
                    state | SCHEDULED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        // If the task is not yet scheduled and isn't currently running, now is the
                        // time to schedule it.
                        if state & RUNNING == 0 {
                            // Schedule the task.
                            Self::schedule(ptr);
                        } else {
                            // Drop the waker.
                            Self::drop_waker(ptr);
                        }

                        break;
                    }
                    Err(s) => state = s,
                }
            }
        }
    }

    /// Wakes a waker by reference.
    unsafe fn wake_by_ref(ptr: *const ()) {
        let raw = ptr as *const Self;
        let mut state = (*raw).header.state.load(Ordering::Acquire);

        loop {
            // If the task is completed or closed, it can't be woken up.
            if state & (COMPLETED | CLOSED) != 0 {
                break;
            }

            // If the task is already scheduled, we just need to synchronize with the thread that
            // will run the task by "publishing" our current view of the memory.
            if state & SCHEDULED != 0 {
                // Update the state without actually modifying it.
                match (*raw).header.state.compare_exchange_weak(
                    state,
                    state,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(s) => state = s,
                }
            } else {
                // If the task is not running, we can schedule right away.
                let new = if state & RUNNING == 0 {
                    (state | SCHEDULED) + REFERENCE
                } else {
                    state | SCHEDULED
                };

                // Mark the task as scheduled.
                match (*raw).header.state.compare_exchange_weak(
                    state,
                    new,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        // If the task is not running, now is the time to schedule.
                        if state & RUNNING == 0 {
                            // If the reference count overflowed, abort.
                            if state > isize::max_value() as usize {
                                abort();
                            }

                            // Schedule the task. There is no need to call `Self::schedule(ptr)`
                            // because the schedule function cannot be destroyed while the waker is
                            // still alive.
                            let task = Task {
                                raw_task: NonNull::new_unchecked(ptr as *mut ()),
                                _marker: PhantomData,
                            };
                            ((*raw).schedule)(task);
                        }

                        break;
                    }
                    Err(s) => state = s,
                }
            }
        }
    }

    /// Clones a waker.
    unsafe fn clone_waker(ptr: *const ()) -> RawWaker {
        let raw = ptr as *const Self;

        // Increment the reference count. With any kind of reference-counted data structure,
        // relaxed ordering is appropriate when incrementing the counter.
        let state = (*raw).header.state.fetch_add(REFERENCE, Ordering::Relaxed);

        // If the reference count overflowed, abort.
        if state > isize::max_value() as usize {
            abort();
        }

        RawWaker::new(ptr, &Self::RAW_WAKER_VTABLE)
    }

    /// Drops a waker.
    ///
    /// This function will decrement the reference count. If it drops down to zero, the associated
    /// join handle has been dropped too, and the task has not been completed, then it will get
    /// scheduled one more time so that its future gets dropped by the executor.
    #[inline]
    unsafe fn drop_waker(ptr: *const ()) {
        let raw = ptr as *const Self;

        // Decrement the reference count.
        let new = (*raw).header.state.fetch_sub(REFERENCE, Ordering::AcqRel) - REFERENCE;

        // If this was the last reference to the task and the `JoinHandle` has been dropped too,
        // then we need to decide how to destroy the task.
        if new & !(REFERENCE - 1) == 0 && new & HANDLE == 0 {
            if new & (COMPLETED | CLOSED) == 0 {
                // If the task was not completed nor closed, close it and schedule one more time so
                // that its future gets dropped by the executor.
                (*raw)
                    .header
                    .state
                    .store(SCHEDULED | CLOSED | REFERENCE, Ordering::Release);
                Self::schedule(ptr);
            } else {
                // Otherwise, destroy the task right away.
                Self::destroy(ptr);
            }
        }
    }

    /// Drops a task.
    ///
    /// This function will decrement the reference count. If it drops down to zero and the
    /// associated join handle has been dropped too, then the task gets destroyed.
    #[inline]
    unsafe fn drop_task(ptr: *const ()) {
        let raw = ptr as *const Self;

        // Decrement the reference count.
        let new = (*raw).header.state.fetch_sub(REFERENCE, Ordering::AcqRel) - REFERENCE;

        // If this was the last reference to the task and the `JoinHandle` has been dropped too,
        // then destroy the task.
        if new & !(REFERENCE - 1) == 0 && new & HANDLE == 0 {
            Self::destroy(ptr);
        }
    }

    /// Schedules a task for running.
    ///
    /// This function doesn't modify the state of the task. It only passes the task reference to
    /// its schedule function.
    unsafe fn schedule(ptr: *const ()) {
        let raw = ptr as *const Self;

        // If the schedule function has captured variables, create a temporary waker that prevents
        // the task from getting deallocated while the function is being invoked.
        let _waker;
        if mem::size_of::<S>() > 0 {
            _waker = Waker::from_raw(Self::clone_waker(ptr));
        }

        let task = Task {
            raw_task: NonNull::new_unchecked(ptr as *mut ()),
            _marker: PhantomData,
        };
        ((*raw).schedule)(task);
    }

    /// Drops the future inside a task.
    #[inline]
    unsafe fn drop_future(ptr: *const ()) {
        let raw = ptr as *mut Self;

        // We need a safeguard against panics because the destructor can panic.
        abort_on_panic(|| (*raw).future_then_output.future().drop_in_place());
    }

    /// Returns a pointer to the output inside a task.
    unsafe fn get_output(ptr: *const ()) -> *const () {
        let raw = ptr as *const Self;
        (*raw).future_then_output.output() as *const R as _
    }

    /// Cleans up task's resources and deallocates it.
    ///
    /// The schedule function and the tag will be dropped, and the task will then get deallocated.
    /// The task must be closed before this function is called.
    #[inline]
    unsafe fn destroy(ptr: *const ()) {
        let raw = ptr as *mut Self;

        // We need a safeguard against panics because destructors can panic.
        abort_on_panic(|| {
            // Drop the schedule function.
            ptr::drop_in_place(&mut (*raw).schedule);

            // Drop the tag.
            ptr::drop_in_place(&mut (*raw).tag);
        });

        // Finally, deallocate the memory reserved by the task.
        let boxed = Box::from_raw(raw as *mut ManuallyDrop<Self>);
        drop(boxed);
    }

    /// Runs a task.
    ///
    /// If polling its future panics, the task will be closed and the panic will be propagated into
    /// the caller.
    unsafe fn run(ptr: *const ()) {
        let raw = ptr as *mut Self;

        // Create a context from the raw task pointer and the vtable inside the its header.
        let waker = ManuallyDrop::new(Waker::from_raw(RawWaker::new(ptr, &Self::RAW_WAKER_VTABLE)));
        let cx = &mut Context::from_waker(&waker);

        let mut state = (*raw).header.state.load(Ordering::Acquire);

        // Update the task's state before polling its future.
        loop {
            // If the task has already been closed, drop the task reference and return.
            if state & CLOSED != 0 {
                // Notify the awaiter that the task has been closed.
                if state & AWAITER != 0 {
                    (*raw).header.notify(None);
                }

                // Drop the future.
                Self::drop_future(ptr);

                // Drop the task reference.
                Self::drop_task(ptr);
                return;
            }

            // Mark the task as unscheduled and running.
            match (*raw).header.state.compare_exchange_weak(
                state,
                (state & !SCHEDULED) | RUNNING,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // Update the state because we're continuing with polling the future.
                    state = (state & !SCHEDULED) | RUNNING;
                    break;
                }
                Err(s) => state = s,
            }
        }

        // Poll the inner future, but surround it with a guard that closes the task in case polling
        // panics.
        let guard = Guard(raw);
        let future = &mut *(*raw).future_then_output.future();
        let poll = <F as Future>::poll(Pin::new_unchecked(future), cx);
        mem::forget(guard);

        match poll {
            Poll::Ready(out) => {
                // Replace the future with its output.
                Self::drop_future(ptr);
                ptr::write(
                    &mut (*raw).future_then_output,
                    FutureThenOutput::Output(out),
                );

                // A place where the output will be stored in case it needs to be dropped.
                let mut output = None;

                // The task is now completed.
                loop {
                    // If the handle is dropped, we'll need to close it and drop the output.
                    let new = if state & HANDLE == 0 {
                        (state & !RUNNING & !SCHEDULED) | COMPLETED | CLOSED
                    } else {
                        (state & !RUNNING & !SCHEDULED) | COMPLETED
                    };

                    // Mark the task as not running and completed.
                    match (*raw).header.state.compare_exchange_weak(
                        state,
                        new,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            // If the handle is dropped or if the task was closed while running,
                            // now it's time to drop the output.
                            if state & HANDLE == 0 || state & CLOSED != 0 {
                                // Read the output.
                                output = Some(ptr::read((*raw).future_then_output.output()));
                            }

                            // Notify the awaiter that the task has been completed.
                            if state & AWAITER != 0 {
                                (*raw).header.notify(None);
                            }

                            // Drop the task reference.
                            Self::drop_task(ptr);
                            break;
                        }
                        Err(s) => state = s,
                    }
                }

                // Drop the output if it was taken out of the task.
                drop(output);
            }
            Poll::Pending => {
                // The task is still not completed.
                loop {
                    // If the task was closed while running, we'll need to unschedule in case it
                    // was woken up and then destroy it.
                    let new = if state & CLOSED != 0 {
                        state & !RUNNING & !SCHEDULED
                    } else {
                        state & !RUNNING
                    };

                    // Mark the task as not running.
                    match (*raw).header.state.compare_exchange_weak(
                        state,
                        new,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(state) => {
                            // If the task was closed while running, we need to drop its future.
                            // If the task was woken up while running, we need to schedule it.
                            // Otherwise, we just drop the task reference.
                            if state & CLOSED != 0 {
                                // The thread that closed the task didn't drop the future because
                                // it was running so now it's our responsibility to do so.
                                Self::drop_future(ptr);

                                // Drop the task reference.
                                Self::drop_task(ptr);
                            } else if state & SCHEDULED != 0 {
                                // The thread that woke the task up didn't reschedule it because
                                // it was running so now it's our responsibility to do so.
                                Self::schedule(ptr);
                            } else {
                                // Drop the task reference.
                                Self::drop_task(ptr);
                            }
                            break;
                        }
                        Err(s) => state = s,
                    }
                }
            }
        }

        /// A guard that closes the task if polling its future panics.
        struct Guard<F, R, S, T>(*mut RawTask<F, R, S, T>)
        where
            F: Future<Output = R> + 'static,
            S: Fn(Task<T>) + Send + Sync + 'static;

        impl<F, R, S, T> Drop for Guard<F, R, S, T>
        where
            F: Future<Output = R> + 'static,
            S: Fn(Task<T>) + Send + Sync + 'static,
        {
            fn drop(&mut self) {
                let raw = self.0;
                let ptr = raw as *const ();

                unsafe {
                    let mut state = (*raw).header.state.load(Ordering::Acquire);

                    loop {
                        // If the task was closed while running, then unschedule it, drop its
                        // future, and drop the task reference.
                        if state & CLOSED != 0 {
                            // We still need to unschedule the task because it is possible it was
                            // woken up while running.
                            (*raw).header.state.fetch_and(!SCHEDULED, Ordering::AcqRel);

                            // The thread that closed the task didn't drop the future because it
                            // was running so now it's our responsibility to do so.
                            RawTask::<F, R, S, T>::drop_future(ptr);

                            // Drop the task reference.
                            RawTask::<F, R, S, T>::drop_task(ptr);
                            break;
                        }

                        // Mark the task as not running, not scheduled, and closed.
                        match (*raw).header.state.compare_exchange_weak(
                            state,
                            (state & !RUNNING & !SCHEDULED) | CLOSED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                            Ok(state) => {
                                // Drop the future because the task is now closed.
                                RawTask::<F, R, S, T>::drop_future(ptr);

                                // Notify the awaiter that the task has been closed.
                                if state & AWAITER != 0 {
                                    (*raw).header.notify(None);
                                }

                                // Drop the task reference.
                                RawTask::<F, R, S, T>::drop_task(ptr);
                                break;
                            }
                            Err(s) => state = s,
                        }
                    }
                }
            }
        }
    }
}
