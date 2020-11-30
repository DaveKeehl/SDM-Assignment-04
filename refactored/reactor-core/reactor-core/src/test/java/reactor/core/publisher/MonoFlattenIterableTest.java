package reactor.core.publisher;

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;
import reactor.util.concurrent.Queues;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class MonoFlattenIterableTest {

	@Test
    void scanOperator() {
		MonoFlattenIterable<Integer, Integer> test =
                new MonoFlattenIterable<>(Mono.just(1), i -> Arrays.asList(i), Integer.MAX_VALUE, Queues.one());

        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

}