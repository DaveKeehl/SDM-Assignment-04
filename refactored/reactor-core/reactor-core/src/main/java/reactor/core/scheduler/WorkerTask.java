/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.scheduler;

import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import reactor.util.annotation.Nullable;

/**
 * A runnable task for {@link Scheduler} Workers that are time-capable (implementing a
 * relevant schedule(delay) and schedulePeriodically(period) methods).
 *
 * Unlike the one in {@link DelegateServiceScheduler}, this runnable doesn't expose the
 * ability to cancel inner task when interrupted.
 *
 * @author Simon Baslé
 * @author David Karnok
 */
final class WorkerTask extends Task {

	/** marker that the Worker was disposed and the parent got notified */
	static final Composite DISPOSED = new EmptyCompositeDisposable();
	/** marker that the Worker has completed, for the PARENT field */
	static final Composite DONE     = new EmptyCompositeDisposable();


	/** marker that the Worker has completed, for the FUTURE field */
	static final Future<Void> FINISHED        = new FutureTask<>(() -> null);
	/**
	 * marker that the Worker was cancelled from the same thread (ie. within call()/run()),
	 * which means setFuture might race: we avoid interrupting the Future in this case.
	 */
	static final Future<Void> SYNC_CANCELLED  = new FutureTask<>(() -> null);
	/**
	 * marker that the Worker was cancelled from another thread, making it safe to
	 * interrupt the Future task.
	 */
	//see https://github.com/reactor/reactor-core/issues/1107
	static final Future<Void> ASYNC_CANCELLED = new FutureTask<>(() -> null);

	volatile Future<?> future;
	static final AtomicReferenceFieldUpdater<WorkerTask, Future> FUTURE_UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(WorkerTask.class, Future.class, "future");

	volatile Composite parent;
	static final AtomicReferenceFieldUpdater<WorkerTask, Composite> PARENT_UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(WorkerTask.class, Composite.class, "parent");

	volatile Thread thread;
	static final AtomicReferenceFieldUpdater<WorkerTask, Thread> THREAD_UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(WorkerTask.class, Thread.class, "thread");

	WorkerTask(Runnable task, Composite parent) {
		super(task);
		PARENT_UPDATER.lazySet(this, parent);
	}

	@Override
	@Nullable
	public Void call() {
		THREAD_UPDATER.lazySet(this, Thread.currentThread());
		try {
			try {
				runnableTask.run();
			}
			catch (Throwable ex) {
				Schedulers.handleError(ex);
			}
		}
		finally {
			THREAD_UPDATER.lazySet(this, null);
			Composite o = parent;
			//note: the o != null check must happen after the compareAndSet for it to always mark task as DONE
			if (o != DISPOSED && PARENT_UPDATER.compareAndSet(this, o, DONE) && o != null) {
				o.remove(this);
			}

			Future<?> f;
			do {
				f = future;
			} while (f != SYNC_CANCELLED && f != ASYNC_CANCELLED && !FUTURE_UPDATER.compareAndSet(this, f, FINISHED));
		}
		return null;
	}

	@Override
	public void run() {
		call();
	}

	void setFuture(Future<?> f) {
		for (;;) {
			Future<?> o = future;
			if (o == FINISHED) {
				return;
			}
			if (o == SYNC_CANCELLED) {
				f.cancel(false);
				return;
			}
			if (o == ASYNC_CANCELLED) {
				f.cancel(true);
				return;
			}
			if (FUTURE_UPDATER.compareAndSet(this, o, f)) {
				return;
			}
		}
	}

	@Override
	public boolean isDisposed() {
		Composite o = PARENT_UPDATER.get(this);
		return o == DISPOSED || o == DONE;
	}

	public void disposeStep1(){
		for (;;) {
			Future<?> f = future;
			boolean async = thread != Thread.currentThread();
			if (f == FINISHED || f == SYNC_CANCELLED || f == ASYNC_CANCELLED
					|| FUTURE_UPDATER.compareAndSet(this, f, async ? ASYNC_CANCELLED : SYNC_CANCELLED)) {
				if (f != null) {
					f.cancel(async);
				}
				break;
			}
		}
	}

	public void disposeStep2(){
		for (;;) {
			Composite o = parent;
			if (o == DONE || o == DISPOSED || o == null) {
				return;
			}
			if (PARENT_UPDATER.compareAndSet(this, o, DISPOSED)) {
				o.remove(this);
				return;
			}
		}
	}

	@Override
	public void dispose() {
		disposeStep1();
		disposeStep2();
	}

}
