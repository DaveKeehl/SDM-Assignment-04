package reactor.core.scheduler;

import reactor.util.annotation.Nullable;

import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public abstract class PeriodicTask extends Task{

    volatile Future<?> future;
    Thread thread;
    static final Future<Void> CANCELLED = new FutureTask<>(() -> null);
    @SuppressWarnings("rawtypes")
    static final AtomicReferenceFieldUpdater<PeriodicTask, Future> ATOMIC_REFERENCE_FUTURE =
            AtomicReferenceFieldUpdater.newUpdater(PeriodicTask.class, Future.class, "future");

    PeriodicTask(Runnable task) {
        super(task);
    }

    @Override
    @Nullable
    public Void call() {
        thread = Thread.currentThread();
        try {
            try {
                runnableTask.run();
            }
            catch (Exception ex) {
                Schedulers.handleError(ex);
            }
        }
        finally {
            thread = null;
        }
        return null;
    }

    @Override
    public void run() {
        call();
    }

    @Override
    public boolean isDisposed() {
        return future == CANCELLED;
    }

    void setFuture(Future<?> f) {
        for (;;) {
            Future<?> o = future;
            if (o == CANCELLED) {
                f.cancel(thread != Thread.currentThread());
                return;
            }
            if (ATOMIC_REFERENCE_FUTURE.compareAndSet(this, o, f)) {
                return;
            }
        }
    }

    @Override
    public void dispose() {
        for (;;) {
            Future<?> f = future;
            if (f == CANCELLED || ATOMIC_REFERENCE_FUTURE.compareAndSet(this, f, CANCELLED)) {
                if (f != null && f != CANCELLED) {
                    f.cancel(thread != Thread.currentThread());
                }
                break;
            }
        }
    }
}
