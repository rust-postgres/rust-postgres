/// Runs a cleanup closure when dropped, unless disarmed first.
///
/// For cleanup that has to happen when a future is cancelled — dropped before
/// being polled to completion — and so cannot be left to the `Drop` impl of a
/// value that does not exist yet. Arm the guard as soon as there is something to
/// clean up, disarm it once something else owns it; early returns via `?` are
/// covered for free.
///
/// ```ignore
/// let resource = create()?;
/// let guard = DropGuard::new(|| destroy(&resource));
///
/// do_work().await?; // may be cancelled, or return early
///
/// guard.disarm();
/// Ok(Owner::new(resource)) // `Owner::drop` destroys it from here on
/// ```
pub struct DropGuard<F: FnOnce()>(Option<F>);

impl<F: FnOnce()> DropGuard<F> {
    pub fn new(f: F) -> DropGuard<F> {
        DropGuard(Some(f))
    }

    /// Gives up responsibility, for when something else has taken it over.
    ///
    /// Drops the closure without calling it, so anything it captured by value
    /// is released normally.
    pub fn disarm(mut self) {
        self.0 = None;
    }
}

impl<F: FnOnce()> Drop for DropGuard<F> {
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f();
        }
    }
}

#[cfg(test)]
mod test {
    use super::DropGuard;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn drop_runs_the_closure() {
        let count = AtomicUsize::new(0);
        {
            let _ = count.fetch_add(1, Ordering::Relaxed);
            let _guard = DropGuard::new(|| {
                let _ = count.fetch_sub(1, Ordering::Relaxed);
            });
        }
        assert_eq!(count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn disarm_skips_the_closure() {
        let count = AtomicUsize::new(0);
        {
            let _ = count.fetch_add(1, Ordering::Relaxed);
            let guard = DropGuard::new(|| {
                let _ = count.fetch_sub(1, Ordering::Relaxed);
            });
            guard.disarm();
        }
        assert_eq!(count.load(Ordering::Relaxed), 1);
    }
}
