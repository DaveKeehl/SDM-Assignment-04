/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;
import reactor.core.publisher.FluxOnAssembly.AssemblySnapshot;

import static org.assertj.core.api.Assertions.assertThat;

class MonoCallableOnAssemblyTest {

	@Test
    void scanOperator() {
		AssemblySnapshot stacktrace = new AssemblySnapshot(null, Traces.callSiteSupplierFactory.get());
		Mono<Object> source = Mono.empty();
		MonoCallableOnAssembly<?> test = new MonoCallableOnAssembly<>(source, stacktrace);

		assertThat(test.scan(Scannable.Attr.ACTUAL_METADATA)).isTrue();
		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
    void stepNameAndToString() {
		AssemblySnapshot stacktrace = new AssemblySnapshot(null, Traces.callSiteSupplierFactory.get());
		MonoCallableOnAssembly<?> test = new MonoCallableOnAssembly<>(Mono.empty(), stacktrace);

		assertThat(test.toString())
				.isEqualTo(test.stepName())
				.startsWith("reactor.core.publisher.MonoCallableOnAssemblyTest.stepNameAndToString(MonoCallableOnAssemblyTest.java:");
	}

}
