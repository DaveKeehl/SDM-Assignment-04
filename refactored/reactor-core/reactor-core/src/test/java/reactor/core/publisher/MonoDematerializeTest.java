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

import org.junit.jupiter.api.Test;
import java.time.Duration;

import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.PublisherProbe;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

class MonoDematerializeTest {

	Signal<Integer> error = Signal.error(new RuntimeException("Forced failure"));

	@Test
    void valueSignal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono<Integer> dematerialize = Mono.just(Signal.next(1))
		                                  .dematerialize();

		dematerialize.subscribe(ts);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
    void completionSignal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono<Integer> dematerialize = Mono.just(Signal.<Integer>complete())
		                                  .dematerialize();

		dematerialize.subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
    void errorSignal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono<Integer> dematerialize = Mono.just(error)
		                                  .dematerialize();

		dematerialize.subscribe(ts);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertNotComplete();
	}


	@Test
    void valueNeedsRequestOne() {
		Mono.just(1).materialize().log().block();

		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Mono<Integer> dematerialize = Mono.just(Signal.next(1))
		                                  .dematerialize();

		dematerialize.subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);
		ts.assertValues(1)
		  .assertComplete();
	}

	@Test
    void completionNeedsRequestOne() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Mono<Integer> dematerialize = Mono.just(Signal.<Integer>complete())
		                                  .dematerialize();

		dematerialize.subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);
		ts.assertComplete();
	}

	@Test
    void errorNeedsRequestOne() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Mono<Integer> dematerialize = Mono.just(error)
		                                  .dematerialize();

		dematerialize.subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);
		ts.assertError(RuntimeException.class);
	}

	@Test
    void emptyMaterializedMono() {
		StepVerifier.create(Mono.empty().dematerialize())
		            .verifyComplete();
	}

	@Test
    void failingMaterializedMono() {
		StepVerifier.create(Mono.error(new IllegalStateException("boom")).dematerialize())
		            .verifyErrorMessage("boom");
	}

	@Test
    void emptyMaterializedMonoDoesntNeedRequest() {
		StepVerifier.create(Mono.empty().dematerialize(), 0)
		            .expectSubscription()
		            .verifyComplete();
	}

	@Test
    void failingMaterializedMonoDoesntNeedRequest() {
		StepVerifier.create(Mono.error(new IllegalStateException("boom")).dematerialize(), 0)
		            .expectSubscription()
		            .verifyErrorMessage("boom");
	}

	@Test
    void neverSignalSource() {
		Mono<Integer> dematerialize = Mono.never()
		                                  .dematerialize();

		StepVerifier.create(dematerialize, 0)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(50))
		            .thenRequest(1)
		            .expectNoEvent(Duration.ofMillis(50))
		            .thenCancel()
		            .verify();
	}

	@Test
    void sourceWithSignalButNeverCompletes() {
		//the noncompliant TestPublisher should result in Mono.fromDirect, preventing it from sending an onComplete
		TestPublisher<String> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);

		Mono<String> neverEndingSource = testPublisher.mono();

		StepVerifier.create(neverEndingSource.materialize().dematerialize())
		            .expectSubscription()
		            .then(() -> testPublisher.next("foo"))
		            .expectNext("foo")
		            .verifyComplete();

		testPublisher.assertWasNotCancelled(); //new behavior as of 3.4.0 for MonoMaterialize
	}

	@Test
    void infiniteEmptyMaterializeDematerializePropagatesCancel() {
		final PublisherProbe<Object> of = PublisherProbe.of(Mono.never());
		StepVerifier.create(of.mono().materialize().dematerialize())
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(100))
		            .thenCancel()
		            .verify();
		of.assertWasCancelled();
	}

	@Test
    void materializeDematerializeMonoEmpty() {
		StepVerifier.create(Mono.empty().materialize().dematerialize())
		            .verifyComplete();
	}

	@Test
    void materializeDematerializeMonoJust() {
		StepVerifier.create(Mono.just("foo").materialize().dematerialize())
		            .expectNext("foo")
		            .verifyComplete();
	}

	@Test
    void materializeDematerializeMonoError() {
		StepVerifier.create(Mono.error(new IllegalStateException("boom")).materialize().dematerialize())
		            .verifyErrorMessage("boom");
	}

	@Test
    void scanOperator(){
	    MonoDematerialize<Integer> test = new MonoDematerialize<>(Mono.just(Signal.next(1)));

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}
