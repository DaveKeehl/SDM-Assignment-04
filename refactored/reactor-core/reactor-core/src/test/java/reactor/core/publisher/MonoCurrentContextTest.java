package reactor.core.publisher;

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;

import static org.assertj.core.api.Assertions.assertThat;

@Deprecated
class MonoCurrentContextTest {

    @Test
    void scanOperator(){
        MonoCurrentContext test = MonoCurrentContext.INSTANCE;

        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
    }

}