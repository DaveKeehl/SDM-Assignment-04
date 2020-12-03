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

package reactor.core.publisher;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Scheduler.Worker;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Emits events on a different thread specified by a scheduler callback.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxPublishOn<T> extends InternalFluxOperator<T, T> implements Fuseable {

	final Scheduler scheduler;

	final boolean delayError;

	final Supplier<? extends Queue<T>> queueSupplier;

	final int prefetch;

	final int lowTide;

	FluxPublishOn(Flux<? extends T> source,
			Scheduler scheduler,
			boolean delayError,
			int prefetch,
			int lowTide,
			Supplier<? extends Queue<T>> queueSupplier) {
		super(source);
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
		this.delayError = delayError;
		this.prefetch = prefetch;
		this.lowTide = lowTide;
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_ON) return scheduler;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

		return super.scanUnsafe(key);
	}

	@Override
	public int getPrefetch() {
		return prefetch;
	}

	@Override
	@SuppressWarnings("unchecked")
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		Worker worker = Objects.requireNonNull(scheduler.createWorker(),
				"The scheduler returned a null worker");

		if (actual instanceof ConditionalSubscriber) {
			ConditionalSubscriber<? super T> cs = (ConditionalSubscriber<? super T>) actual;
			source.subscribe(new PublishOnConditionalSubscriber<>(cs,
					scheduler,
					worker,
					delayError,
					prefetch,
					lowTide,
					queueSupplier));
			return null;
		}
		return new PublishOnSubscriber<>(actual,
				scheduler,
				worker,
				delayError,
				prefetch,
				lowTide,
				queueSupplier);
	}

	abstract static class AbstractPublishOnSubscriber<T>
			implements QueueSubscription<T>, Runnable, InnerOperator<T, T> {
		final CoreSubscriber<? super T> actual;

		final Worker worker;

		final Scheduler scheduler;

		final boolean delayError;

		final int prefetch;

		final int limit;

		final Supplier<? extends Queue<T>> queueSupplier;

		Subscription s;

		Queue<T> queue;

		volatile boolean cancelled;

		volatile boolean done;

		Throwable error;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<AbstractPublishOnSubscriber> WIP_UPDATER =
				AtomicIntegerFieldUpdater.newUpdater(AbstractPublishOnSubscriber.class,
						"wip");

		volatile int discardGuard;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<AbstractPublishOnSubscriber> DISCARD_GUARD_UPDATER =
				AtomicIntegerFieldUpdater.newUpdater(AbstractPublishOnSubscriber.class,
						"discardGuard");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<AbstractPublishOnSubscriber> ATOMIC_LONG_REQUESTED_UPDATER =
				AtomicLongFieldUpdater.newUpdater(AbstractPublishOnSubscriber.class,
						"requested");

		int sourceMode;

		long produced;

		long consumed;

		boolean outputFused;

		AbstractPublishOnSubscriber(CoreSubscriber<? super T> actual,
							Scheduler scheduler,
							Worker worker,
							boolean delayError,
							int prefetch,
							int lowTide,
							Supplier<? extends Queue<T>> queueSupplier) {
			this.actual = actual;
			this.worker = worker;
			this.scheduler = scheduler;
			this.delayError = delayError;
			this.prefetch = prefetch;
			this.queueSupplier = queueSupplier;
			this.limit = Operators.unboundedOrLimit(prefetch, lowTide);
		}

		abstract void onDiscardQueueWithClear();
		abstract void onError(RejectedExecutionException ree,
					 @Nullable Subscription subscription,
					 @Nullable Throwable suppressed,
					 @Nullable T dataSignal);

		abstract void onDiscard(@Nullable T dataSignal);
		abstract Throwable onOperatorError(Throwable ex);

		Context getActualCurrentContext(){
			return actual.currentContext();
		}

		void doComplete(Subscriber<?> a) {
			a.onComplete();
			worker.dispose();
		}

		void doError(Subscriber<?> a, Throwable e) {
			try {
				a.onError(e);
			}
			finally {
				worker.dispose();
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				if (s instanceof QueueSubscription) {
					@SuppressWarnings("unchecked") QueueSubscription<T> f =
							(QueueSubscription<T>) s;

					int m = f.requestFusion(Fuseable.ANY | Fuseable.THREAD_BARRIER);

					if (m == Fuseable.SYNC) {
						sourceMode = Fuseable.SYNC;
						queue = f;
						done = true;

						actual.onSubscribe(this);
						return;
					}
					if (m == Fuseable.ASYNC) {
						sourceMode = Fuseable.ASYNC;
						queue = f;

						actual.onSubscribe(this);

						s.request(Operators.unboundedOrPrefetch(prefetch));

						return;
					}
				}

				queue = queueSupplier.get();

				actual.onSubscribe(this);

				s.request(Operators.unboundedOrPrefetch(prefetch));
			}
		}

		@SuppressWarnings("rawtypes")
		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM ) return requested;
			if (key == Attr.PARENT ) return s;
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.BUFFERED) return queue != null ? queue.size() : 0;
			if (key == Attr.ERROR) return error;
			if (key == Attr.DELAY_ERROR) return delayError;
			if (key == Attr.PREFETCH) return prefetch;
			if (key == Attr.RUN_ON) return worker;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void cancel() {
			if (cancelled) {
				return;
			}

			cancelled = true;
			s.cancel();
			worker.dispose();

			if (WIP_UPDATER.getAndIncrement(this) != 0) {
				return;
			}

			if (sourceMode == ASYNC) {
				// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
				queue.clear();
			}
			else if (!outputFused) {
				// discard MUST be happening only and only if there is no racing on elements consumption
				// which is guaranteed by the WIP_UPDATER guard here in case non-fused output
				onDiscardQueueWithClear();
			}
		}

		@Override
		public void clear() {
			// use guard on the queue instance as the best way to ensure there is no racing on draining
			// the call to this method must be done only during the ASYNC fusion so all the callers will be waiting
			// this should not be performance costly with the assumption the cancel is rare operation
			if (DISCARD_GUARD_UPDATER.getAndIncrement(this) != 0) {
				return;
			}

			int missed = 1;

			for (;;) {
				onDiscardQueueWithClear();

				int dg = discardGuard;
				if (missed == dg) {
					missed = DISCARD_GUARD_UPDATER.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				}
				else {
					missed = dg;
				}
			}
		}

		void trySchedule(
				@Nullable Subscription subscription,
				@Nullable Throwable suppressed,
				@Nullable T dataSignal) {
			if (WIP_UPDATER.getAndIncrement(this) != 0) {
				if (cancelled) {
					if (sourceMode == ASYNC) {
						// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
						queue.clear();
					}
					else {
						// discard given dataSignal since no more is enqueued (spec guarantees serialised onXXX calls)
						onDiscard(dataSignal);
					}
				}
				return;
			}

			try {
				worker.schedule(this);
			}
			catch (RejectedExecutionException ree) {
				if (sourceMode == ASYNC) {
					// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
					queue.clear();
				} else if (outputFused) {
					// We are the holder of the queue, but we still have to perform discarding under the guarded block
					// to prevent any racing done by downstream
					this.clear();
				}
				else {
					// In all other modes we are free to discard queue immediately since there is no racing on pooling
					onDiscardQueueWithClear();
				}
				onError(ree, subscription, suppressed, dataSignal);
			}
		}

		@Override
		@Nullable
		public T poll() {
			T v = queue.poll();
			if (v != null && sourceMode != SYNC) {
				long p = produced + 1;
				if (p == limit) {
					produced = 0;
					s.request(p);
				}
				else {
					produced = p;
				}
			}
			return v;
		}

		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, @Nullable T v) {
			if (cancelled) {
				onDiscard(v);
				if (sourceMode == ASYNC) {
					// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
					queue.clear();
				} else {
					// discard MUST be happening only and only if there is no racing on elements consumption
					// which is guaranteed by the WIP_UPDATER guard here
					onDiscardQueueWithClear();
				}
				return true;
			}
			if (d) {
				if (delayError) {
					if (empty) {
						Throwable e = error;
						if (e != null) {
							doError(a, e);
						}
						else {
							doComplete(a);
						}
						return true;
					}
				}
				else {
					Throwable e = error;
					if (e != null) {
						onDiscard(v);
						if (sourceMode == ASYNC) {
							// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
							queue.clear();
						} else {
							// discard MUST be happening only and only if there is no racing on elements consumption
							// which is guaranteed by the WIP_UPDATER guard here
							onDiscardQueueWithClear();
						}
						doError(a, e);
						return true;
					}
					else if (empty) {
						doComplete(a);
						return true;
					}
				}
			}

			return false;
		}

		void runBackfused() {
			int missed = 1;

			do {

				if (cancelled) {
					// We are the holder of the queue, but we still have to perform discarding under the guarded block
					// to prevent any racing done by downstream
					this.clear();
					return;
				}

				boolean d = done;

				actual.onNext(null);

				if (d) {
					Throwable e = error;
					if (e != null) {
						doError(actual, e);
					} else {
						doComplete(actual);
					}
					return;
				}

				missed = WIP_UPDATER.addAndGet(this, -missed);
			} while (missed != 0);
		}

		void runSync() {
			int missed = 1;

			final Subscriber<? super T> a = actual;
			final Queue<T> q = queue;

			long e = produced;

			for (; ; ) {

				long r = requested;

				while (e != r) {
					T v;

					try {
						v = q.poll();
					}
					catch (Throwable ex) {
						doError(a, onOperatorError(ex));
						return;
					}

					if (cancelled) {
						onDiscard(v);
						onDiscardQueueWithClear();
						return;
					}
					if (v == null) {
						doComplete(a);
						return;
					}

					a.onNext(v);

					e++;
				}

				if (cancelled) {
					onDiscardQueueWithClear();
					return;
				}

				if (q.isEmpty()) {
					doComplete(a);
					return;
				}

				int w = wip;
				if (missed == w) {
					produced = e;
					missed = WIP_UPDATER.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				}
				else {
					missed = w;
				}
			}
		}

		@Override
		public void onNext(T t) {
			if (sourceMode == ASYNC) {
				trySchedule(this, null, null /* t always null */);
				return;
			}

			if (done) {
				Operators.onNextDropped(t, getActualCurrentContext());
				return;
			}

			if (cancelled) {
				Operators.onDiscard(t, getActualCurrentContext());
				return;
			}

			if (!queue.offer(t)) {
				Operators.onDiscard(t, getActualCurrentContext());
				error = Operators.onOperatorError(s,
						Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL),
						t, getActualCurrentContext());
				done = true;
			}
			trySchedule(this, null, t);
		}
	}
	static final class PublishOnSubscriber<T>
			extends AbstractPublishOnSubscriber<T> {

		PublishOnSubscriber(CoreSubscriber<? super T> actual,
				Scheduler scheduler,
				Worker worker,
				boolean delayError,
				int prefetch,
				int lowTide,
				Supplier<? extends Queue<T>> queueSupplier) {
			super(actual, scheduler, worker, delayError, prefetch, lowTide, queueSupplier);
		}

		@Override
		void onDiscardQueueWithClear(){
			Operators.onDiscardQueueWithClear(queue, getActualCurrentContext(), null);
		}

		@Override
		void onError(RejectedExecutionException ree,
					 @Nullable Subscription subscription,
					 @Nullable Throwable suppressed,
					 @Nullable T dataSignal){
			actual.onError(Operators.onRejectedExecution(ree, subscription, suppressed, dataSignal,
					getActualCurrentContext()));
		}

		@Override
		void onDiscard(@Nullable T dataSignal){
			Operators.onDiscard(dataSignal, getActualCurrentContext());
		}

		@Override
		Throwable onOperatorError(Throwable ex){
			return Operators.onOperatorError(s, ex,
					getActualCurrentContext());
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, getActualCurrentContext());
				return;
			}
			error = t;
			done = true;
			trySchedule(null, t, null);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			//WIP_UPDATER also guards, no competing onNext
			trySchedule(null, null, null);
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(ATOMIC_LONG_REQUESTED_UPDATER, this, n);
				//WIP_UPDATER also guards during request and onError is possible
				trySchedule(this, null, null);
			}
		}

		void runAsync() {
			int missed = 1;

			final Subscriber<? super T> a = actual;
			final Queue<T> q = queue;

			long e = produced;

			for (; ; ) {

				long r = requested;

				while (e != r) {
					boolean d = done;
					T v;

					try {
						v = q.poll();
					}
					catch (Throwable ex) {
						Exceptions.throwIfFatal(ex);
						s.cancel();
						if (sourceMode == ASYNC) {
							// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
							queue.clear();
						} else {
							// discard MUST be happening only and only if there is no racing on elements consumption
							// which is guaranteed by the WIP_UPDATER guard here
							Operators.onDiscardQueueWithClear(queue, getActualCurrentContext(), null);
						}

						doError(a, Operators.onOperatorError(ex, getActualCurrentContext()));
						return;
					}

					boolean empty = v == null;

					if (checkTerminated(d, empty, a, v)) {
						return;
					}

					if (empty) {
						break;
					}

					a.onNext(v);

					e++;
					if (e == limit) {
						if (r != Long.MAX_VALUE) {
							r = ATOMIC_LONG_REQUESTED_UPDATER.addAndGet(this, -e);
						}
						s.request(e);
						e = 0L;
					}
				}

				if (e == r && checkTerminated(done, q.isEmpty(), a, null)) {
					return;
				}

				int w = wip;
				if (missed == w) {
					produced = e;
					missed = WIP_UPDATER.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				}
				else {
					missed = w;
				}
			}
		}

		@Override
		public void run() {
			if (outputFused) {
				runBackfused();
			}
			else if (sourceMode == Fuseable.SYNC) {
				runSync();
			}
			else {
				runAsync();
			}
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public boolean isEmpty() {
			return queue.isEmpty();
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & ASYNC) != 0) {
				outputFused = true;
				return ASYNC;
			}
			return NONE;
		}

		@Override
		public int size() {
			return queue.size();
		}
	}

	static final class PublishOnConditionalSubscriber<T>
			extends AbstractPublishOnSubscriber<T> {

		PublishOnConditionalSubscriber(ConditionalSubscriber<? super T> actual,
				Scheduler scheduler,
				Worker worker,
				boolean delayError,
				int prefetch,
				int lowTide,
				Supplier<? extends Queue<T>> queueSupplier) {
			super(actual, scheduler, worker, delayError, prefetch, lowTide, queueSupplier);
		}

		@Override
		void onDiscardQueueWithClear(){
			Operators.onDiscardQueueWithClear(queue, getActualCurrentContext(), null);
		}

		@Override
		void onError(RejectedExecutionException ree,
					 @Nullable Subscription subscription,
					 @Nullable Throwable suppressed,
					 @Nullable T dataSignal){
			actual.onError(Operators.onRejectedExecution(ree, subscription, suppressed, dataSignal,
					getActualCurrentContext()));
		}

		@Override
		void onDiscard(@Nullable T dataSignal){
			Operators.onDiscard(dataSignal, getActualCurrentContext());
		}

		@Override
		Throwable onOperatorError(Throwable ex){
			return Operators.onOperatorError(s, ex,
					getActualCurrentContext());
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, getActualCurrentContext());
				return;
			}
			error = t;
			done = true;
			trySchedule(null, t, null);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			trySchedule(null, null, null);
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(ATOMIC_LONG_REQUESTED_UPDATER, this, n);
				trySchedule(this, null, null);
			}
		}

		@Override
		public void cancel() {
			if (cancelled) {
				return;
			}

			cancelled = true;
			s.cancel();
			worker.dispose();

			if (WIP_UPDATER.getAndIncrement(this) == 0) {
				if (sourceMode == ASYNC) {
					// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
					queue.clear();
				}
				else if (!outputFused) {
					// discard MUST be happening only and only if there is no racing on elements consumption
					// which is guaranteed by the WIP_UPDATER guard here in case non-fused output
					Operators.onDiscardQueueWithClear(queue, getActualCurrentContext(), null);
				}
			}
		}

		@Override
		void trySchedule(
				@Nullable Subscription subscription,
				@Nullable Throwable suppressed,
				@Nullable T dataSignal) {
			if (WIP_UPDATER.getAndIncrement(this) != 0) {
				if (cancelled) {
					if (sourceMode == ASYNC) {
						// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
						queue.clear();
					}
					else {
						// discard given dataSignal since no more is enqueued (spec guarantees serialised onXXX calls)
						Operators.onDiscard(dataSignal, getActualCurrentContext());
					}
				}
				return;
			}

			try {
				worker.schedule(this);
			}
			catch (RejectedExecutionException ree) {
				if (sourceMode == ASYNC) {
					// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
					queue.clear();
				} else if (outputFused) {
					// We are the holder of the queue, but we still have to perform discarding under the guarded block
					// to prevent any racing done by downstream
					this.clear();
				}
				else {
					// In all other modes we are free to discard queue immediately since there is no racing on pooling
					Operators.onDiscardQueueWithClear(queue, getActualCurrentContext(), null);
				}
				actual.onError(Operators.onRejectedExecution(ree, subscription, suppressed, dataSignal,
						getActualCurrentContext()));
			}
		}

		void runAsync() {
			int missed = 1;

			final ConditionalSubscriber<? super T> a = (ConditionalSubscriber<? super T>) actual;
			final Queue<T> q = queue;

			long emitted = produced;
			long polled = consumed;

			for (; ; ) {

				long r = requested;

				while (emitted != r) {
					boolean d = done;
					T v;
					try {
						v = q.poll();
					}
					catch (Throwable ex) {
						Exceptions.throwIfFatal(ex);
						s.cancel();
						// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
						q.clear();

						doError(a, Operators.onOperatorError(ex, getActualCurrentContext()));
						return;
					}
					boolean empty = v == null;

					if (checkTerminated(d, empty, a, v)) {
						return;
					}

					if (empty) {
						break;
					}

					if (a.tryOnNext(v)) {
						emitted++;
					}

					polled++;

					if (polled == limit) {
						s.request(polled);
						polled = 0L;
					}
				}

				if (emitted == r && checkTerminated(done, q.isEmpty(), a, null)) {
					return;
				}

				int w = wip;
				if (missed == w) {
					produced = emitted;
					consumed = polled;
					missed = WIP_UPDATER.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				}
				else {
					missed = w;
				}
			}

		}

		@Override
		public void run() {
			if (outputFused) {
				runBackfused();
			}
			else if (sourceMode == Fuseable.SYNC) {
				runSync();
			}
			else {
				runAsync();
			}
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public boolean isEmpty() {
			return queue.isEmpty();
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & ASYNC) != 0) {
				outputFused = true;
				return ASYNC;
			}
			return NONE;
		}

		@Override
		public int size() {
			return queue.size();
		}
	}
}
