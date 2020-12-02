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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import static org.assertj.core.api.Assertions.assertThat;

class MonoFilterWhenTest {

	@Test
    void normalFiltered() {
		StepVerifier.withVirtualTime(() -> Mono.just(1)
		                                       .filterWhen(v -> Mono.just(v % 2 == 0)
		                                                            .delayElement(Duration.ofMillis(100))))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(100))
		            .verifyComplete();
	}

	@Test
    void normalNotFiltered() {
		StepVerifier.withVirtualTime(() -> Mono.just(2)
		                                       .filterWhen(v -> Mono.just(v % 2 == 0)
		                                                            .delayElement(Duration.ofMillis(100))))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(100))
		            .expectNext(2)
		            .verifyComplete();
	}

	@Test
    void normalSyncFiltered() {
		StepVerifier.create(Mono.just(1)
		                        .filterWhen(v -> Mono.just(v % 2 == 0).hide()))
	                .verifyComplete();
	}

	@Test
    void normalSyncNotFiltered() {
		StepVerifier.create(Mono.just(2)
		                        .filterWhen(v -> Mono.just(v % 2 == 0).hide()))
		            .expectNext(2)
	                .verifyComplete();
	}

	@Test
    void normalSyncFusedFiltered() {
		StepVerifier.create(Mono.just(1)
		                        .filterWhen(v -> Mono.just(v % 2 == 0)))
	                .verifyComplete();
	}

	@Test
    void normalSyncFusedNotFiltered() {
		StepVerifier.create(Mono.just(2)
		                        .filterWhen(v -> Mono.just(v % 2 == 0)))
		            .expectNext(2)
	                .verifyComplete();
	}

	@Test
    void allEmpty() {
		StepVerifier.create(Mono.just(2)
		                        .filterWhen(v -> Mono.<Boolean>empty().hide()))
		            .verifyComplete();
	}

	@Test
    void allEmptyFused() {
		StepVerifier.create(Mono.just(2)
		                        .filterWhen(v -> Mono.empty()))
		            .verifyComplete();
	}

	@Test
    void empty() {
		StepVerifier.create(Mono.<Integer>empty()
								.filterWhen(v -> Mono.just(true)))
		            .verifyComplete();
	}

	@Test
    void emptyBackpressured() {
		StepVerifier.create(Mono.<Integer>empty()
				.filterWhen(v -> Mono.just(true)), 0L)
				.verifyComplete();
	}

	@Test
    void error() {
		StepVerifier.create(Mono.<Integer>error(new IllegalStateException())
				.filterWhen(v -> Mono.just(true)))
				.verifyError(IllegalStateException.class);
	}

	@Test
    void errorBackpressured() {
		StepVerifier.create(Mono.<Integer>error(new IllegalStateException())
				.filterWhen(v -> Mono.just(true)), 0L)
				.verifyError(IllegalStateException.class);
	}

	@Test
    void backpressureExactlyOne() {
		StepVerifier.create(Mono.just(1)
		                        .filterWhen(v -> Mono.just(true)), 1L)
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
    void oneAndErrorInner() {
		StepVerifier.create(Mono.just(1)
		                        .filterWhen(v -> s -> {
			                        s.onSubscribe(Operators.emptySubscription());
			                        s.onNext(true);
			                        s.onError(new IllegalStateException());
		                        }))
		            .expectNext(1)
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDroppedErrorsSatisfying(
				            c -> assertThat(c)
						            .hasSize(1)
						            .element(0).isInstanceOf(IllegalStateException.class)
		            );
	}

	@Test
    void predicateThrows() {
		StepVerifier.create(Mono.just(1)
		                        .filterWhen(v -> { throw new IllegalStateException(); }))
		            .verifyError(IllegalStateException.class);
	}

	@Test
    void predicateNull() {
		StepVerifier.create(Mono.just(1).filterWhen(v -> null))
		            .verifyError(NullPointerException.class);
	}

	@Test
    void predicateError() {
		StepVerifier.create(Mono.just(1)
		                        .filterWhen(v -> Mono.<Boolean>error(new IllegalStateException()).hide()))
		            .verifyError(IllegalStateException.class);
	}


	@Test
    void predicateErrorFused() {
		StepVerifier.create(Mono.just(1)
		                        .filterWhen(v -> Mono.fromCallable(() -> { throw new IllegalStateException(); })))
		            .verifyError(IllegalStateException.class);
	}

	@Test
    void take1Cancel() {
		AtomicLong onNextCount = new AtomicLong();
		AtomicReference<SignalType> endSignal = new AtomicReference<>();
		BaseSubscriber<Object> bs = new BaseSubscriber<Object>() {

			@Override
			protected void hookOnSubscribe(@NotNull Subscription subscription) {
				requestUnbounded();
			}

			@Override
			public void hookOnNext(@NotNull Object t) {
				onNextCount.incrementAndGet();
				cancel();
				onComplete();
			}

			@Override
			protected void hookFinally(SignalType type) {
				endSignal.set(type);
			}
		};

		Mono.just(1)
		    .filterWhen(v -> Mono.just(true).hide())
		    .subscribe(bs);

		assertThat(onNextCount).hasValue(1);
		assertThat(endSignal).hasValue(SignalType.CANCEL);
	}

	@Test
    void take1CancelBackpressured() {
		AtomicLong onNextCount = new AtomicLong();
		AtomicReference<SignalType> endSignal = new AtomicReference<>();
		BaseSubscriber<Object> bs = new BaseSubscriber<Object>() {

			@Override
			protected void hookOnSubscribe(@NotNull Subscription subscription) {
				request(1);
			}

			@Override
			public void hookOnNext(@NotNull Object t) {
				onNextCount.incrementAndGet();
				cancel();
				onComplete();
			}

			@Override
			protected void hookFinally(SignalType type) {
				endSignal.set(type);
			}
		};

		Mono.just(1)
		        .filterWhen(v -> Mono.just(true).hide())
		        .subscribe(bs);

		assertThat(onNextCount).hasValue(1);
		assertThat(endSignal).hasValue(SignalType.CANCEL);
	}


	@Test
    void cancel() {
		final Sinks.Many<Boolean> pp = Sinks.many().multicast().onBackpressureBuffer();

		StepVerifier.create(Mono.just(1)
		                        .filterWhen(v -> pp.asFlux()))
		            .thenCancel();

		assertThat(pp.currentSubscriberCount()).as("pp has subscriber").isZero();
	}

	@Test
    void innerFluxCancelled() {
		AtomicInteger cancelCount = new AtomicInteger();

		StepVerifier.create(Mono.just(1)
		                        .filterWhen(v -> Flux.just(true, false, false)
		                                             .doOnCancel(cancelCount::incrementAndGet)))
		            .expectNext(1)
		            .verifyComplete();

		assertThat(cancelCount).hasValue(1);
	}

	@Test
    void innerFluxOnlyConsidersFirstValue() {
		StepVerifier.create(Mono.just(1)
		                        .filterWhen(v -> Flux.just(false, true, true)))
		            .verifyComplete();
	}

	@Test
    void innerMonoNotCancelled() {
		AtomicInteger cancelCount = new AtomicInteger();

		StepVerifier.create(Mono.just(3)
		                        .filterWhen(v -> Mono.just(true)
		                                             .doOnCancel(cancelCount::incrementAndGet)))
		            .expectNext(3)
		            .verifyComplete();

		assertThat(cancelCount).hasValue(0);
	}

	@Test
    void scanTerminatedOnlyTrueIfFilterTerminated() {
		AtomicReference<Subscriber> subscriber = new AtomicReference<>();
		TestPublisher<Boolean> filter = TestPublisher.create();
		new MonoFilterWhen<>(new Mono<Integer>() {
			@Override
			public void subscribe(CoreSubscriber<? super Integer> actual) {
				subscriber.set(actual);
				//NON-EMPTY SOURCE WILL TRIGGER FILTER SUBSCRIPTION
				actual.onNext(2);
				actual.onComplete();
			}
		}, w -> filter)
	        .subscribe();

		assertThat(subscriber.get()).isNotNull()
	                                .isInstanceOf(Scannable.class);
		Boolean terminated = ((Scannable) subscriber.get()).scan(Scannable.Attr.TERMINATED);
		assertThat(terminated).isFalse();

		filter.emit(Boolean.TRUE);

		terminated = ((Scannable) subscriber.get()).scan(Scannable.Attr.TERMINATED);
		assertThat(terminated).isTrue();
	}

	@Test
    void scanOperator(){
	    MonoFilterWhen<Integer> test = new MonoFilterWhen<>(Mono.just(1), null);

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
    void scanSubscriber() {
		CoreSubscriber<String> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoFilterWhen.MonoFilterWhenMain<String>
				test = new MonoFilterWhen.MonoFilterWhenMain<>(
				actual, s -> Mono.just(false));
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		//TERMINATED IS COVERED BY TEST ABOVE

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
    void scanFilterWhenInner() {
		CoreSubscriber<String> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoFilterWhen.MonoFilterWhenMain<String>
				main = new MonoFilterWhen.MonoFilterWhenMain<>(
				actual, s -> Mono.just(false));
		MonoFilterWhen.FilterWhenInner test = new MonoFilterWhen.FilterWhenInner(main, true);

		Subscription innerSubscription = Operators.emptySubscription();
		test.onSubscribe(innerSubscription);

		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(innerSubscription);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(1L);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(0L);

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

}
