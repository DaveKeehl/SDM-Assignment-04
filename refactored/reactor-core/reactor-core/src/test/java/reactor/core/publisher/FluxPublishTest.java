/*
 * Copyright (c) 2011-Present Pivotal Software Inc, All Rights Reserved.
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

class FluxPublishTest extends FluxOperatorTest<String, String> {

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.prefetch(Queues.SMALL_BUFFER_SIZE);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.publish().autoConnect()),

				scenario(f -> f.publish().refCount())
		);
	}

	@Test
    void failPrefetch() {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> {
			Flux.never()
					.publish(-1);
		});
	}

	@Test
    void prematureOnComplete() {
		Sinks.Many<Flux<String>> sink = Sinks.unsafe().many().multicast().onBackpressureBuffer(Queues.SMALL_BUFFER_SIZE, false);

		Flux.just("ALPHA", "BRAVO", "CHARLIE", "DELTA", "ALPHA", "BRAVO", "CHARLIE", "DELTA", "ALPHA", "BRAVO", "CHARLIE", "DELTA")
		    .log("stream.incoming")
		    .windowWhile(s -> !"DELTA".equals(s),1 )
		    .subscribe(v -> sink.emitNext(v, FAIL_FAST), e -> sink.emitError(e, FAIL_FAST), () -> sink.emitComplete(FAIL_FAST));

		AtomicInteger windowIndex = new AtomicInteger(0);
		AtomicInteger nextIndex = new AtomicInteger(0);

		sink.asFlux()
				.next()
				.flatMapMany(flux -> flux
						.takeWhile(s -> !"CHARLIE".equals(s))
						.log(String.format("stream.window.%d", windowIndex.getAndIncrement())))
				.log(String.format("stream.next.%d", nextIndex.getAndIncrement()))
				.as(StepVerifier::create)
				.expectNextCount(2)
				.verifyComplete();

		sink.asFlux().next()
		                 .flatMapMany(flux -> flux
				                 .takeWhile(s -> !"CHARLIE".equals(s))
				                 .log(String.format("stream.window.%d", windowIndex.getAndIncrement())))
		                 .log(String.format("stream.next.%d", nextIndex.getAndIncrement()))
		                 .as(StepVerifier::create)
		                 .expectNextCount(2)
		                 .verifyComplete();

		sink.asFlux().next()
		                 .flatMapMany(flux -> flux
				                 .takeWhile(s -> !"CHARLIE".equals(s))
				                 .log(String.format("stream.window.%d", windowIndex.getAndIncrement())))
		                 .log(String.format("stream.next.%d", nextIndex.getAndIncrement()))
		                 .as(StepVerifier::create)
		                 .expectNextCount(2)
		                 .verifyComplete();
	}

	/*@Test
    void constructors() {
		ConstructorTestBuilder ctb = new ConstructorTestBuilder(StreamPublish.class);

		ctb.addRef("source", Flux.never());
		ctb.addInt("prefetch", 1, Integer.MAX_VALUE);
		ctb.addRef("queueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());

		ctb.test();
	}*/

	@Test
    void normal() {
		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();

		ConnectableFlux<Integer> p = Flux.range(1, 5).hide().publish();

		p.subscribe(ts1);
		p.subscribe(ts2);

		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts2
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		p.connect();

		ts1.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();

		ts2.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();
	}

	@Test
    void normalBackpressured() {
		AssertSubscriber<Integer> ts1 = AssertSubscriber.create(0);
		AssertSubscriber<Integer> ts2 = AssertSubscriber.create(0);

		ConnectableFlux<Integer> p = Flux.range(1, 5).hide().publish();

		p.subscribe(ts1);
		p.subscribe(ts2);

		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts2
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		p.connect();

		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts2
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts1.request(3);
		ts2.request(2);

		ts1.assertValues(1, 2)
		.assertNoError()
		.assertNotComplete();

		ts2.assertValues(1, 2)
		.assertNoError()
		.assertNotComplete();

		ts1.request(2);
		ts2.request(3);

		ts1.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();

		ts2.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();
	}

	@Test
    void normalAsyncFused() {
		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();

		Sinks.Many<Integer> up = Sinks.unsafe().many().unicast().onBackpressureBuffer(Queues.<Integer>get(8).get());
		up.emitNext(1, FAIL_FAST);
		up.emitNext(2, FAIL_FAST);
		up.emitNext(3, FAIL_FAST);
		up.emitNext(4, FAIL_FAST);
		up.emitNext(5, FAIL_FAST);
		up.emitComplete(FAIL_FAST);

		ConnectableFlux<Integer> p = up.asFlux().publish();

		p.subscribe(ts1);
		p.subscribe(ts2);

		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts2
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		p.connect();

		ts1.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();

		ts2.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();
	}

	@Test
    void normalBackpressuredAsyncFused() {
		AssertSubscriber<Integer> ts1 = AssertSubscriber.create(0);
		AssertSubscriber<Integer> ts2 = AssertSubscriber.create(0);

		Sinks.Many<Integer> up = Sinks.unsafe().many().unicast().onBackpressureBuffer(Queues.<Integer>get(8).get());
		up.emitNext(1, FAIL_FAST);
		up.emitNext(2, FAIL_FAST);
		up.emitNext(3, FAIL_FAST);
		up.emitNext(4, FAIL_FAST);
		up.emitNext(5, FAIL_FAST);
		up.emitComplete(FAIL_FAST);

		ConnectableFlux<Integer> p = up.asFlux().publish();

		p.subscribe(ts1);
		p.subscribe(ts2);

		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts2
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		p.connect();

		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts2
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts1.request(3);
		ts2.request(2);

		ts1.assertValues(1, 2)
		.assertNoError()
		.assertNotComplete();

		ts2.assertValues(1, 2)
		.assertNoError()
		.assertNotComplete();

		ts1.request(2);
		ts2.request(3);

		ts1.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();

		ts2.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();
	}

	@Test
    void normalSyncFused() {
		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();

		ConnectableFlux<Integer> p = Flux.range(1, 5).publish(5);

		p.subscribe(ts1);
		p.subscribe(ts2);

		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts2
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		p.connect();

		ts1.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();

		ts2.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();
	}

	@Test
    void normalBackpressuredSyncFused() {
		AssertSubscriber<Integer> ts1 = AssertSubscriber.create(0);
		AssertSubscriber<Integer> ts2 = AssertSubscriber.create(0);

		ConnectableFlux<Integer> p = Flux.range(1, 5).publish(5);

		p.subscribe(ts1);
		p.subscribe(ts2);

		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts2
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		p.connect();

		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts2
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts1.request(3);
		ts2.request(2);

		ts1.assertValues(1, 2)
		.assertNoError()
		.assertNotComplete();

		ts2.assertValues(1, 2)
		.assertNoError()
		.assertNotComplete();

		ts1.request(2);
		ts2.request(3);

		ts1.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();

		ts2.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();
	}

	//see https://github.com/reactor/reactor-core/issues/1302
	@Test
    void boundaryFused() {
		Flux.range(1, 10000)
		    .publishOn(Schedulers.single())
		    .map(v -> Thread.currentThread().getName().contains("single-") ? "single" : ("BAD-" + v + Thread.currentThread().getName()))
		    .share()
		    .publishOn(Schedulers.boundedElastic())
		    .distinct()
		    .as(StepVerifier::create)
		    .expectFusion()
		    .expectNext("single")
		    .expectComplete()
		    .verify(Duration.ofSeconds(5));
	}

	@Test
    void disconnect() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> e = Sinks.many().multicast().onBackpressureBuffer();

		ConnectableFlux<Integer> p = e.asFlux().publish();

		p.subscribe(ts);

		Disposable r = p.connect();

		e.emitNext(1, FAIL_FAST);
		e.emitNext(2, FAIL_FAST);

		r.dispose();

		ts.assertValues(1, 2)
		.assertError(CancellationException.class)
		.assertNotComplete();

		assertThat(e.currentSubscriberCount()).as("still connected").isZero();
	}

	@Test
    void disconnectBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Sinks.Many<Integer> e = Sinks.many().multicast().onBackpressureBuffer();

		ConnectableFlux<Integer> p = e.asFlux().publish();

		p.subscribe(ts);

		Disposable r = p.connect();

		r.dispose();

		ts.assertNoValues()
		.assertError(CancellationException.class)
		.assertNotComplete();

		assertThat(e.currentSubscriberCount()).as("still connected").isZero();
	}

	@Test
    void error() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> e = Sinks.many().multicast().onBackpressureBuffer();

		ConnectableFlux<Integer> p = e.asFlux().publish();

		p.subscribe(ts);

		p.connect();

		e.emitNext(1, FAIL_FAST);
		e.emitNext(2, FAIL_FAST);
		e.emitError(new RuntimeException("forced failure"), FAIL_FAST);

		ts.assertValues(1, 2)
				.assertError(RuntimeException.class)
				.assertErrorWith(x -> assertThat(x).hasMessageContaining("forced failure"))
				.assertNotComplete();
	}

	@Test
    void fusedMapInvalid() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		ConnectableFlux<Integer> p = Flux.range(1, 5).map(v -> (Integer)null).publish();

		p.subscribe(ts);

		p.connect();

		ts.assertNoValues()
		.assertError(NullPointerException.class)
		.assertNotComplete();
	}


	@Test
    void retry() {
		Sinks.Many<Integer> dp = Sinks.unsafe().many().multicast().directBestEffort();
		StepVerifier.create(
				dp.asFlux()
				  .publish()
				  .autoConnect().<Integer>handle((s1, sink) -> {
					if (s1 == 1) {
						sink.error(new RuntimeException());
					}
					else {
						sink.next(s1);
					}
				}).retry())
		            .then(() -> {
			            dp.emitNext(1, FAIL_FAST);
			            dp.emitNext(2, FAIL_FAST);
			            dp.emitNext(3, FAIL_FAST);
		            })
		            .expectNext(2, 3)
		            .thenCancel()
		            .verify();

		// Need to explicitly complete processor due to use of publish()
		dp.emitComplete(FAIL_FAST);
	}

	@Test
    void retryWithPublishOn() {
		Sinks.Many<Integer> dp = Sinks.unsafe().many().multicast().directBestEffort();
		StepVerifier.create(
				dp.asFlux()
				  .publishOn(Schedulers.parallel()).publish()
				  .autoConnect().<Integer>handle((s1, sink) -> {
					if (s1 == 1) {
						sink.error(new RuntimeException());
					}
					else {
						sink.next(s1);
					}
				}).retry())
		            .then(() -> {
			            dp.emitNext(1, FAIL_FAST);
			            dp.emitNext(2, FAIL_FAST);
			            dp.emitNext(3, FAIL_FAST);
		            })
		            .expectNext(2, 3)
		            .thenCancel()
		            .verify();

		// Need to explicitly complete processor due to use of publish()
		dp.emitComplete(FAIL_FAST);
	}

	@Test
    void scanMain() {
        Flux<Integer> parent = Flux.just(1).map(i -> i);
        FluxPublish<Integer> test = new FluxPublish<>(parent, 123, Queues.unbounded());

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(123);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
    }

	@Test
    void scanSubscriber() {
        FluxPublish<Integer> main = new FluxPublish<>(Flux.just(1), 123, Queues.unbounded());
        FluxPublish.PublishSubscriber<Integer> test = new FluxPublish.PublishSubscriber<>(789, main);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(789);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
        test.queue.add(5);
        assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        assertThat(test.scan(Scannable.Attr.ERROR)).isNull();
        test.error = new IllegalArgumentException("boom");
        assertThat(test.scan(Scannable.Attr.ERROR)).isSameAs(test.error);
        test.onComplete();
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

        test = new FluxPublish.PublishSubscriber<>(789, main);
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.onSubscribe(Operators.cancelledSubscription());
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

	@Test
    void scanInner() {
		FluxPublish<Integer> main = new FluxPublish<>(Flux.just(1), 123, Queues.unbounded());
        FluxPublish.PublishSubscriber<Integer> parent = new FluxPublish.PublishSubscriber<>(789, main);
        Subscription sub = Operators.emptySubscription();
        parent.onSubscribe(sub);
        FluxPublish.PublishInner<Integer> test = new FluxPublish.PublishInner<>(parent);

        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(parent);
        test.parent = parent;
        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        test.request(35);
        assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        parent.terminate();
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

	@Test
    void scanPubSubInner() {
		FluxPublish<Integer> main = new FluxPublish<>(Flux.just(1), 123, Queues.unbounded());
        FluxPublish.PublishSubscriber<Integer> parent = new FluxPublish.PublishSubscriber<>(789, main);
        Subscription sub = Operators.emptySubscription();
        parent.onSubscribe(sub);
        FluxPublish.PubSubInner<Integer> test = new FluxPublish.PublishInner<>(parent);

        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
        test.request(35);
        assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

    //see https://github.com/reactor/reactor-core/issues/1290
    @Test
    void syncFusionSingle() { //single value in the SYNC fusion
	    final ConnectableFlux<String> publish = Flux.just("foo")
	                                 .publish();

		StepVerifier.create(publish)
		            .then(publish::connect)
		            .expectNext("foo")
		            .expectComplete()
		            .verify(Duration.ofSeconds(4));
    }

    //see https://github.com/reactor/reactor-core/issues/1290
	@Test
    void syncFusionMultiple() { //multiple values in the SYNC fusion
		final ConnectableFlux<Integer> publish = Flux.range(1, 5)
		                                             .publish();

		StepVerifier.create(publish)
		            .then(publish::connect)
		            .expectNext(1, 2, 3, 4, 5)
		            .expectComplete()
		            .verify(Duration.ofSeconds(4));
	}

	//see https://github.com/reactor/reactor-core/issues/1528
	@Test
	@Timeout(4)
	void syncFusionFromInfiniteStream() {
		final ConnectableFlux<Integer> publish =
				Flux.fromStream(Stream.iterate(0, i -> i + 1))
				    .publish();

		StepVerifier.create(publish)
		            .then(publish::connect)
		            .thenConsumeWhile(i -> i < 10)
		            .expectNextCount(10)
		            .thenCancel()
		            .verify(Duration.ofSeconds(4));
	}

	//see https://github.com/reactor/reactor-core/issues/1528
	@Test
	@Timeout(4)
	void syncFusionFromInfiniteStreamAndTake() {
		final Flux<Integer> publish =
				Flux.fromStream(Stream.iterate(0, i -> i + 1))
				    .publish()
				    .autoConnect()
				    .take(10);

		StepVerifier.create(publish)
		            .expectNextCount(10)
		            .expectComplete()
		            .verify(Duration.ofSeconds(4));
	}

	@Test
    void dataDroppedIfConnectImmediately() {
		TestPublisher<Integer> publisher = TestPublisher.create();
		ConnectableFlux<Integer> connectableFlux = publisher.flux().publish();

		connectableFlux.connect();

		publisher.next(1);
		publisher.next(2);
		publisher.next(3);

		StepVerifier.create(connectableFlux)
		            .expectSubscription()
		            .then(() -> publisher.next(99))
		            .expectNext(99)
		            .then(publisher::complete)
		            .verifyComplete();
	}

	@Test
    void dataDroppedIfAutoconnectZero() {
		TestPublisher<Integer> publisher = TestPublisher.create();
		Flux<Integer> flux = publisher.flux().publish().autoConnect(0);

		publisher.next(1);
		publisher.next(2);
		publisher.next(3);

		StepVerifier.create(flux)
		            .expectSubscription()
		            .then(() -> publisher.next(99))
		            .expectNext(99)
		            .then(publisher::complete)
		            .verifyComplete();
	}

	@Test
    void removeUnknownInnerIgnored() {
		FluxPublish.PublishSubscriber<Integer> subscriber = new FluxPublish.PublishSubscriber<>(1, null);
		FluxPublish.PublishInner<Integer> inner = new FluxPublish.PublishInner<>(null);
		FluxPublish.PublishInner<Integer> notInner = new FluxPublish.PublishInner<>(null);

		subscriber.add(inner);
		assertThat(subscriber.subscribers).as("adding inner").hasSize(1);

		subscriber.remove(notInner);
		assertThat(subscriber.subscribers).as("post remove notInner").hasSize(1);

		subscriber.remove(inner);
		assertThat(subscriber.subscribers).as("post remove inner").isEmpty();
	}

	@Test
    void subscriberContextPropagation() {
		String key = "key";
		int expectedValue = 1;

		AtomicReference<ContextView> reference = new AtomicReference<>();

		Flux<Integer> integerFlux =
				Flux.just(1, 2, 3)
				    .flatMap(value ->
						    Mono.deferContextual(Mono::just)
						        .doOnNext(reference::set)
						        .thenReturn(value)
				    )
				    .publish()
				    .autoConnect(2);

		integerFlux.contextWrite(Context.of(key, expectedValue))
		           .subscribe();

		integerFlux.contextWrite(Context.of(key, 2))
		           .subscribe();

		assertThat((int) reference.get().get(key)).isEqualTo(expectedValue);
	}
}
