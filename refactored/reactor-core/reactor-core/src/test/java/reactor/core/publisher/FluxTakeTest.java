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

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.MockUtils;
import reactor.test.StepVerifier;
import reactor.test.publisher.PublisherProbe;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

class FluxTakeTest {

	@Test
    void sourceNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new FluxTake<>(null, 1);
		});
	}

	@Test
    void numberIsInvalid() {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> {
			Flux.never()
					.take(-1);
		});
	}

	@Test
    void numberIsInvalidFused() {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> {
			Flux.just(1)
					.take(-1);
		});
	}

	@Test
    void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .take(5)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
    void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .take(5)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
    void takeZero() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .take(0)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertComplete()
		  .assertNoError();
	}

	@Test
    void takeNever() {
		StepVerifier.create(
				Flux.never().take(1))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(1))
		            .thenCancel()
		            .verify();
	}

	@Test
    void takeNeverZero() {
		PublisherProbe<Object> probe = PublisherProbe.of(Flux.never());
		StepVerifier.create(probe.flux().take(0))
		            .expectSubscription()
		            .expectComplete()
		            .verify(Duration.ofSeconds(1));

		probe.assertWasCancelled();
	}

	@Test
    void takeOverflowAttempt() {
		Publisher<Integer> p = s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onNext(1);
			s.onNext(2);
			s.onNext(3);
		};

		StepVerifier.create(Flux.from(p).take(2))
		            .expectNext(1, 2)
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDroppedExactly(3);
	}

	@Test
    void aFluxCanBeLimited(){
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .take(2)
		)
		            .expectNext("test", "test2")
		            .verifyComplete();
	}

	@Test
    void takeBackpressured() {
		StepVerifier.create(Flux.from(s -> {
			s.onSubscribe(new Subscription() {
				@Override
				public void request(long n) {
					for(int i = 0 ; i < n; i ++) {
						s.onNext("test");
					}
				}

				boolean extraNext = true;

				@Override
				public void cancel() {
					if(extraNext){
						extraNext = false;
						s.onNext("test");
					}
				}
			});
		})
		                        .take(3), 0)
		            .thenAwait()
		            .thenRequest(2)
		            .expectNext("test", "test")
		            .thenRequest(1)
		            .expectNext("test")
		            .verifyComplete();
	}

	@Test
    void takeFusedBackpressured() {
		Sinks.Many<String> up = Sinks.many().unicast().onBackpressureBuffer();
		StepVerifier.create(up.asFlux()
							  .take(3), 0)
		            .expectFusion()
		            .then(() -> up.emitNext("test", FAIL_FAST))
		            .then(() -> up.emitNext("test2", FAIL_FAST))
		            .thenRequest(2)
		            .expectNext("test", "test2")
		            .then(() -> up.emitNext("test3", FAIL_FAST))
		            .then(() -> up.emitNext("test4", FAIL_FAST))
		            .thenRequest(1)
		            .expectNext("test3")
		            .thenRequest(1)
		            .verifyComplete();
	}

	@Test
    void takeFusedBackpressuredCancelled() {
		Sinks.Many<String> up = Sinks.many().unicast().onBackpressureBuffer();
		StepVerifier.create(up.asFlux()
							  .take(3).doOnSubscribe(s -> {
			assertThat(((Fuseable.QueueSubscription)s).size()).isEqualTo(0);
		}), 0)
		            .expectFusion()
		            .then(() -> up.emitNext("test", FAIL_FAST))
		            .then(() -> up.emitNext("test", FAIL_FAST))
		            .then(() -> up.emitNext("test", FAIL_FAST))
		            .thenRequest(2)
		            .expectNext("test", "test")
		            .thenCancel()
					.verify();
	}


	@Test
    void takeBackpressuredConditional() {
		StepVerifier.create(Flux.from(s -> {
			s.onSubscribe(new Subscription() {
				@Override
				public void request(long n) {
					for(int i = 0 ; i < n; i ++) {
						s.onNext("test");
					}
				}

				boolean extraNext = true;

				@Override
				public void cancel() {
					if(extraNext){
						extraNext = false;
						s.onNext("test");
					}
				}
			});
		})
		                        .take(3)
								.filter("test"::equals), 0)
		            .thenAwait()
		            .thenRequest(2)
		            .expectNext("test", "test")
		            .thenRequest(1)
		            .expectNext("test")
		            .verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked")
	void takeBackpressuredSourceConditional() {
		StepVerifier.create(Flux.from(_s -> {
			Fuseable.ConditionalSubscriber s = (Fuseable.ConditionalSubscriber)_s;

			s.onSubscribe(new Subscription() {
				@Override
				public void request(long n) {
					for(int i = 0 ; i < n; i ++) {
						s.tryOnNext("test");
					}
				}

				boolean extraNext = true;

				@Override
				public void cancel() {
					if(extraNext){
						extraNext = false;
						s.tryOnNext("test");
					}
				}
			});
		})
		                        .take(3)
								.filter("test"::equals), 0)
		            .thenAwait()
		            .thenRequest(2)
		            .expectNext("test", "test")
		            .thenRequest(1)
		            .expectNext("test")
		            .verifyComplete();
	}

	@Test
    void failNextIfTerminatedTake() {
		Hooks.onNextDropped(t -> assertThat(t).isEqualTo(1));
		StepVerifier.create(Flux.from(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onComplete();
			s.onNext(1);
		})
		                        .take(2))
		            .verifyComplete();
		Hooks.resetOnNextDropped();
	}

	@Test
    void failNextIfTerminatedTakeFused() {
		TestPublisher<Integer> up = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		Hooks.onNextDropped(t -> assertThat(t).isEqualTo(1));
		StepVerifier.create(up.flux().take(2))
		            .then(up::complete)
		            .then(() -> up.next(1))
		            .verifyComplete();
		Hooks.resetOnNextDropped();
	}

	@Test
    void failNextIfTerminatedTakeConditional() {
		Hooks.onNextDropped(t -> assertThat(t).isEqualTo(1));
		StepVerifier.create(Flux.from(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onComplete();
			s.onNext(1);
		})
		                        .take(2)
		                        .filter("test2"::equals))
		            .verifyComplete();
		Hooks.resetOnNextDropped();
	}

	@Test // fixme when we have a fuseable testPublisher or an improved hide operator
	@SuppressWarnings("unchecked")
	void failNextIfTerminatedTakeSourceConditional() {
		Hooks.onNextDropped(t -> assertThat(t).isEqualTo(1));
		StepVerifier.create(Flux.from(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onComplete();
			((Fuseable.ConditionalSubscriber)s).tryOnNext(1);
		})
		                        .take(2)
		                        .filter("test2"::equals))
		            .verifyComplete();
		Hooks.resetOnNextDropped();
	}

	@Test
    void take() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .hide()
		                        .take(2))
		            .expectNext("test", "test2")
		            .verifyComplete();
	}

	@Test
    void takeCancel() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .hide()
		                        .take(3), 2)
		            .expectNext("test", "test2")
		            .thenCancel()
		            .verify();
	}

	@Test
    void takeFused() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .take(2))
		            .expectNext("test", "test2")
		            .verifyComplete();
	}

	@Test
    void takeFusedSync() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .take(2))
		            .expectFusion(Fuseable.SYNC)
		            .expectNext("test", "test2")
		            .verifyComplete();
	}

	@Test
    void takeFusedAsync() {
		Sinks.Many<String> up = Sinks.many().unicast().onBackpressureBuffer();
		StepVerifier.create(up.asFlux()
							  .take(2))
		            .expectFusion(Fuseable.ASYNC)
		            .then(() -> {
			            up.emitNext("test", FAIL_FAST);
			            up.emitNext("test2", FAIL_FAST);
		            })
		            .expectNext("test", "test2")
		            .verifyComplete();
	}

	@Test
    void takeFusedCancel() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .take(3), 2)
		            .expectNext("test", "test2")
		            .thenCancel()
		            .verify();
	}


	@Test
    void takeConditional() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .hide()
		                        .take(2)
		                        .filter("test2"::equals))
		            .expectNext("test2")
		            .verifyComplete();
	}

	@Test
    void takeConditionalCancel() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .hide()
		                        .take(3)
		                        .filter("test2"::equals), 2)
		            .thenCancel()
		            .verify();
	}

	@Test
    void takeConditionalFused() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .take(2)
		                        .filter("test2"::equals))
		            .expectNext("test2")
		            .verifyComplete();
	}

	@Test
    void takeConditionalFusedCancel() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .take(3)
		                        .filter("test2"::equals), 2)
		            .expectNext("test2")
		            .thenCancel()
		            .verify();
	}

	@SuppressWarnings("unchecked")
	void assertTrackableBeforeOnSubscribe(InnerOperator t){
		assertThat(t.scan(Scannable.Attr.TERMINATED)).isFalse();
	}

	void assertTrackableAfterOnSubscribe(InnerOperator t){
		assertThat(t.scan(Scannable.Attr.TERMINATED)).isFalse();
	}

	void assertTrackableAfterOnComplete(InnerOperator t){
		assertThat(t.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	@SuppressWarnings("unchecked")
	void failDoubleError() {
		Hooks.onErrorDropped(e -> assertThat(e).hasMessage("test2"));
		StepVerifier.create(Flux.from(s -> {
			assertTrackableBeforeOnSubscribe((InnerOperator)s);
			s.onSubscribe(Operators.emptySubscription());
			assertTrackableAfterOnSubscribe((InnerOperator)s);
			s.onError(new Exception("test"));
			assertTrackableAfterOnComplete((InnerOperator)s);
			s.onError(new Exception("test2"));
		})
		                        .take(2))
		            .verifyErrorMessage("test");
	}

	@Test
	@SuppressWarnings("unchecked")
	void failConditionalDoubleError() {
		Hooks.onErrorDropped(e -> assertThat(e).hasMessage("test2"));
		StepVerifier.create(Flux.from(s -> {
			assertTrackableBeforeOnSubscribe((InnerOperator)s);
			s.onSubscribe(Operators.emptySubscription());
			assertTrackableAfterOnSubscribe((InnerOperator)s);
			s.onError(new Exception("test"));
			assertTrackableAfterOnComplete((InnerOperator)s);
			s.onError(new Exception("test2"));
		})
		                        .take(2).filter(d -> true))
		            .verifyErrorMessage("test");
	}


	@Test
	@SuppressWarnings("unchecked")
	void failFusedDoubleError() {
		Sinks.Many<Integer> up = Sinks.many().unicast().onBackpressureBuffer();
		Hooks.onErrorDropped(e -> assertThat(e).hasMessage("test2"));
		StepVerifier.create(up.asFlux()
							  .take(2))
		            .consumeSubscriptionWith(s -> {
			            assertTrackableBeforeOnSubscribe((InnerOperator)s);
		            })
		            .then(() -> {
		            	InnerOperator processorDownstream = (InnerOperator) Scannable.from(up).scan(Scannable.Attr.ACTUAL);
			            assertTrackableAfterOnSubscribe(processorDownstream);
			            processorDownstream.onError(new Exception("test"));
			            assertTrackableAfterOnComplete(processorDownstream);
			            processorDownstream.onError(new Exception("test2"));
		            })
		            .verifyErrorMessage("test");
	}

	@Test
    void ignoreFusedDoubleComplete() {
		Sinks.Many<Integer> up = Sinks.many().unicast().onBackpressureBuffer();
		StepVerifier.create(up.asFlux()
							  .take(2).filter(d -> true))
		            .consumeSubscriptionWith(s -> {
			            assertTrackableAfterOnSubscribe((InnerOperator)s);
		            })
		            .then(() -> {
			            InnerOperator processorDownstream = (InnerOperator) Scannable.from(up).scan(Scannable.Attr.ACTUAL);
			            assertTrackableAfterOnSubscribe(processorDownstream);
			            processorDownstream.onComplete();
			            assertTrackableAfterOnComplete(processorDownstream);
			            processorDownstream.onComplete();
		            })
		            .verifyComplete();
	}

	@Test
    void ignoreDoubleComplete() {
		StepVerifier.create(Flux.from(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onComplete();
			s.onComplete();
		})
		                        .take(2))
		            .verifyComplete();
	}

	@Test
    void assertPrefetch() {
		assertThat(Flux.just("test", "test2", "test3")
		               .hide()
		               .take(2)
		               .getPrefetch()).isEqualTo(Integer.MAX_VALUE);
	}

	@Test
    void ignoreDoubleOnSubscribe() {
		StepVerifier.create(Flux.from(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onSubscribe(Operators.emptySubscription());
			s.onComplete();
		})
		                        .take(2))
		            .verifyComplete();
	}
	@Test
    void ignoreConditionalDoubleOnSubscribe() {
		StepVerifier.create(Flux.from(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onSubscribe(Operators.emptySubscription());
			s.onComplete();
		})
		                        .take(2)
		                        .filter(d -> true))
		            .verifyComplete();
	}

	@Test
    void takeZeroCancelsWhenNoRequest() {
		TestPublisher<Integer> ts = TestPublisher.create();
		StepVerifier.create(ts.flux()
		                      .take(0), 0)
		            .thenAwait()
		            .verifyComplete();

		ts.assertWasNotRequested();
		ts.assertWasCancelled();
	}

	@Test
    void takeZeroIgnoresRequestAndCancels() {
		TestPublisher<Integer> ts = TestPublisher.create();
		StepVerifier.create(ts.flux()
		                      .take(0), 3)
		            .thenAwait()
		            .verifyComplete();

		ts.assertWasNotRequested();
		ts.assertWasCancelled();
	}

	@Test
    void takeConditionalZeroCancelsWhenNoRequest() {
		TestPublisher<Integer> ts = TestPublisher.create();
		StepVerifier.create(ts.flux()
		                      .take(0)
		                      .filter(d -> true), 0)
		            .thenAwait()
		            .verifyComplete();

		ts.assertWasNotRequested();
		ts.assertWasCancelled();
	}

	@Test
    void takeConditionalZeroIgnoresRequestAndCancels() {
		TestPublisher<Integer> ts = TestPublisher.create();
		StepVerifier.create(ts.flux()
		                      .take(0)
		                      .filter(d -> true), 3)
		            .thenAwait()
		            .verifyComplete();

		ts.assertWasNotRequested();
		ts.assertWasCancelled();
	}

	@Test
    void scanOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxTake<Integer> test = new FluxTake<>(parent, 3);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
    void scanFuseableOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxTakeFuseable<Integer> test = new FluxTakeFuseable<>(parent, 3);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
    void scanSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxTake.TakeSubscriber<Integer> test = new FluxTake.TakeSubscriber<>(actual, 5);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		Assertions.assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onComplete();
        Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
    }

	@Test
    void scanConditionalSubscriber() {
		@SuppressWarnings("unchecked")
		Fuseable.ConditionalSubscriber<Integer> actual = Mockito.mock(MockUtils.TestScannableConditionalSubscriber.class);
		FluxTake.TakeConditionalSubscriber<Integer> test = new FluxTake.TakeConditionalSubscriber<>(actual, 5);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		Assertions.assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onComplete();
        Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
    }

    @Test
    void scanFuseableSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxTake.TakeFuseableSubscriber<Integer> test = new FluxTake.TakeFuseableSubscriber<>(actual, 10);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
    }

    @Test
    void onSubscribeRaceRequestingShouldBeConsistentForTakeFuseableTest() throws InterruptedException {
		for (int i = 0; i < 5; i++) {
			int take = 3000;
			RaceSubscriber<Integer> actual = new RaceSubscriber<>(take);
			Flux.range(0, Integer.MAX_VALUE)
			    .take(take)
			    .subscribe(actual);

			actual.await(5, TimeUnit.SECONDS);
		}
    }

	@Test
    void onSubscribeRaceRequestingShouldBeConsistentForTakeConditionalTest() throws InterruptedException {
		for (int i = 0; i < 5; i++) {
			int take = 3000;
			RaceSubscriber<Integer> actual = new RaceSubscriber<>(take);
			Flux.range(0, Integer.MAX_VALUE)
			    .take(take)
			    .filter(e -> true)
			    .subscribe(actual);

			actual.await(5, TimeUnit.SECONDS);
		}
	}

	@Test
    void onSubscribeRaceRequestingShouldBeConsistentForTakeTest() throws InterruptedException {
		for (int i = 0; i < 5; i++) {
			int take = 3000;
			RaceSubscriber<Integer> actual = new RaceSubscriber<>(take);
			Flux.range(0, Integer.MAX_VALUE)
			    .hide()
			    .take(take)
			    .subscribe(actual);

			actual.await(5, TimeUnit.SECONDS);
		}
	}

    static final class RaceSubscriber<T> extends BaseSubscriber<T> {
	    final CountDownLatch countDownLatch = new CountDownLatch(1);
	    final int take;
	    int received;

	    RaceSubscriber(int take) {
		    this.take = take;
	    }

	    @Override
		public void hookOnSubscribe(@NotNull Subscription s) {
			CountDownLatch countDownLatch = new CountDownLatch(take);
			for (int i = 0; i < take; i++) {
				new Thread(() -> {
					countDownLatch.countDown();
					try {
						countDownLatch.await();
					}
					catch (InterruptedException e) {
						e.printStackTrace();
					}
					s.request(1);
				}).start();
			}
		}

		@Override
		public void hookOnNext(@NotNull T element) {
			received++;
		}

		@Override
		public void hookOnComplete() {
			assertThat(received).isEqualTo(take);
			countDownLatch.countDown();
		}

		void await(int timeout, TimeUnit unit) throws InterruptedException {
			if (!countDownLatch.await(timeout, unit)) {
				throw new RuntimeException("Expected Completion within "+ timeout +
						" " + unit.name() + " but Complete signal was not emitted");
			}
		}
	}
}
