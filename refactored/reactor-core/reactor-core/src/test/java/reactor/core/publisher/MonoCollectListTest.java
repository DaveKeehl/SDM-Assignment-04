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
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.MonoCollectList.MonoCollectListSubscriber;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.util.RaceTestUtils;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.test.publisher.TestPublisher.Violation.CLEANUP_ON_TERMINATE;

class MonoCollectListTest {

	static final Logger LOGGER = Loggers.getLogger(MonoCollectListTest.class);

	@Test
    void aFluxCanBeSorted(){
		List<Integer> vals = Flux.just(43, 32122, 422, 321, 43, 443311)
		                         .collectSortedList()
		                         .block();

		assertThat(vals).containsExactly(43, 43, 321, 422, 32122, 443311);
	}

	@Test
    void aFluxCanBeSorted2(){
		List<Integer> vals = Flux.just(1, 2, 3, 4)
		                         .collectSortedList(Comparator.reverseOrder())
		                         .block();

		assertThat(vals).containsExactly(4,3,2,1);
	}

	@Test
    void aFluxCanBeSorted3(){
		StepVerifier.create(Flux.just(43, 32122, 422, 321, 43, 443311)
		                        .sort(Comparator.reverseOrder()))
		            .expectNext(443311, 32122, 422, 321, 43, 43)
		            .verifyComplete();
	}

	@Test
    void aFluxCanBeSorted4(){
		StepVerifier.create(Flux.just(43, 32122, 422, 321, 43, 443311)
		                        .sort())
		            .expectNext(43, 43, 321, 422, 32122, 443311)
		            .verifyComplete();
	}

	@Test
    void collectListOne() {
		StepVerifier.create(Flux.just(1)
		                        .collectList())
		            .assertNext(d -> assertThat(d).containsExactly(1))
	                .verifyComplete();

	}

	@Test
    void collectListEmpty() {
		StepVerifier.create(Flux.empty()
		                        .collectList())
		            .assertNext(d -> assertThat(d).isEmpty())
	                .verifyComplete();

	}

	@Test
    void collectListCallable() {
		StepVerifier.create(Mono.fromCallable(() -> 1)
		                        .flux()
		                        .collectList())
		            .assertNext(d -> assertThat(d).containsExactly(1))
	                .verifyComplete();

	}

	@Test
    void collectListError() {
		StepVerifier.create(Flux.error(new Exception("test"))
		                        .collectList())
		            .verifyErrorMessage("test");
	}

	@Test
    void collectListErrorHide() {
		StepVerifier.create(Flux.error(new Exception("test"))
		                        .hide()
		                        .collectList())
		            .verifyErrorMessage("test");
	}

	//see https://github.com/reactor/reactor-core/issues/1523
	@Test
    void protocolErrorsOnNext() {
		TestPublisher<String> testPublisher = TestPublisher.createNoncompliant(CLEANUP_ON_TERMINATE);

		StepVerifier.create(testPublisher.flux().collectList())
		            .expectSubscription()
		            .then(() -> testPublisher.emit("foo"))
		            .then(() -> testPublisher.next("bar"))
		            .assertNext(l -> assertThat(l).containsExactly("foo"))
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDropped("bar");
	}

	//see https://github.com/reactor/reactor-core/issues/1523
	@Test
    void protocolErrorsOnError() {
		TestPublisher<String> testPublisher = TestPublisher.createNoncompliant(CLEANUP_ON_TERMINATE);

		StepVerifier.create(testPublisher.flux().collectList())
		            .expectSubscription()
		            .then(() -> testPublisher.emit("foo"))
		            .then(() -> testPublisher.error(new IllegalStateException("boom")))
		            .assertNext(l -> assertThat(l).containsExactly("foo"))
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDroppedErrorOfType(IllegalStateException.class);
	}

	@Test
    void scanOperator(){
	    Flux<Integer> source = Flux.just(1);
		MonoCollectList<Integer> test = new MonoCollectList<>(source);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
    void scanBufferAllSubscriber() {
		CoreSubscriber<List<String>> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoCollectListSubscriber<String> test = new MonoCollectListSubscriber<>(actual);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
    void discardOnError() {
		Mono<List<Integer>> test = Flux.range(1, 10)
		                               .hide()
		                               .map(i -> {
			                               if (i == 5) throw new IllegalStateException("boom");
			                               return i;
		                               })
		                               .collectList();

		StepVerifier.create(test)
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1, 2, 3, 4);
	}

	@Test
    void discardOnCancel() {
		Mono<List<Long>> test = Flux.interval(Duration.ofMillis(100))
		                            .take(10)
		                            .collectList();

		StepVerifier.create(test)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(210))
		            .thenCancel()
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(0L, 1L);
	}


	@Test
    void discardCancelNextRace() {
		AtomicInteger doubleDiscardCounter = new AtomicInteger();
		Context discardingContext = Operators.enableOnDiscard(null, o -> {
			AtomicBoolean ab = (AtomicBoolean) o;
			if (ab.getAndSet(true)) {
				doubleDiscardCounter.incrementAndGet();
				throw new RuntimeException("test");
			}
		});
		for (int i = 0; i < 100_000; i++) {
			AssertSubscriber<List<AtomicBoolean>> testSubscriber = new AssertSubscriber<>(discardingContext);
			MonoCollectListSubscriber<AtomicBoolean> subscriber = new MonoCollectListSubscriber<>(testSubscriber);
			subscriber.onSubscribe(Operators.emptySubscription());

			AtomicBoolean extraneous = new AtomicBoolean(false);

			RaceTestUtils.race(subscriber::cancel,
					() -> subscriber.onNext(extraneous));

			testSubscriber.assertNoValues();
			if (!extraneous.get()) {
				LOGGER.info(""+subscriber.list);
			}
			assertThat(extraneous).as("released %d", i).isTrue();
		}
		LOGGER.info("discarded twice or more: {}", doubleDiscardCounter.get());
	}

	@Test
    void discardCancelCompleteRace() {
		AtomicInteger doubleDiscardCounter = new AtomicInteger();
		Context discardingContext = Operators.enableOnDiscard(null, o -> {
			AtomicBoolean ab = (AtomicBoolean) o;
			if (ab.getAndSet(true)) {
				doubleDiscardCounter.incrementAndGet();
				throw new RuntimeException("test");
			}
		});
		for (int i = 0; i < 100_000; i++) {
			AssertSubscriber<List<AtomicBoolean>> testSubscriber = new AssertSubscriber<>(discardingContext);
			MonoCollectListSubscriber<AtomicBoolean> subscriber = new MonoCollectListSubscriber<>(testSubscriber);
			subscriber.onSubscribe(Operators.emptySubscription());

			AtomicBoolean resource = new AtomicBoolean(false);
			subscriber.onNext(resource);

			RaceTestUtils.race(subscriber::cancel, subscriber::onComplete);

			if (testSubscriber.values().isEmpty()) {
				assertThat(resource).as("not completed and released %d", i).isTrue();
			}
		}
		LOGGER.info("discarded twice or more: {}", doubleDiscardCounter.get());
	}

	@Test
    void emptyCallable() {
		class EmptyFluxCallable extends Flux<Object> implements Callable<Object> {
			@Override
			public Object call() {
				return null;
			}

			@Override
			public void subscribe(CoreSubscriber<? super Object> actual) {
				Flux.empty().subscribe(actual);
			}
		}
		List<Object> result = new EmptyFluxCallable().collectList().block();

		assertThat(result).isEmpty();
	}

}
