package reactor.core.publisher;

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

class MonoIgnorePublisherTest {

    @Test
    void normal() {
        StepVerifier.create(Mono.ignoreElements(Mono.just("foo")))
                .expectSubscription()
                .verifyComplete();
    }

    @Test
    void scanOperator(){
        MonoIgnoreElement<String> test = new MonoIgnoreElement<>(Mono.just("foo"));

        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
    }
}