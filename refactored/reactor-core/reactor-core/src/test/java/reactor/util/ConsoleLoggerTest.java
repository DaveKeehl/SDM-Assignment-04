/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

package reactor.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConsoleLoggerTest {

	private static final RuntimeException CAUSE = new IllegalStateException("cause");

	private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
	private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();

	private Logger logger;

	@BeforeEach
	public void setUp() {
		logger = new Loggers.ConsoleLogger("test", new PrintStream(outContent), new PrintStream(errContent), true);
	}

	@AfterEach
	public void cleanUp() {
		outContent.reset();
		errContent.reset();
	}

	@Test
    void isTraceEnabled() throws Exception {
		assertThat(logger.isTraceEnabled()).isTrue();
	}

	@Test
    void trace() throws Exception {
		logger.trace("message");

		assertThat(errContent.size()).isZero();
		assertThat(outContent.toString()).isEqualTo("[TRACE] (" + Thread.currentThread().getName() + ") message\n");
	}

	@Test
    void trace1() throws Exception {
		logger.trace("message {} {} format", "with", 1);

		assertThat(errContent.size()).isZero();
		assertThat(outContent.toString()).isEqualTo("[TRACE] (" + Thread.currentThread().getName() + ") message with 1 format\n");
	}

	@Test
    void trace2() throws Exception {
		logger.trace("with cause", CAUSE);

		assertThat(errContent.size()).isZero();
		assertThat(outContent.toString())
				.startsWith("[TRACE] (" + Thread.currentThread().getName() + ") with cause - java.lang.IllegalStateException: cause" +
				"\njava.lang.IllegalStateException: cause\n" +
				"\tat reactor.util.ConsoleLoggerTest");
	}

	@Test
    void traceNulls() {
		logger.trace("vararg {} is {}", (Object[]) null);
		logger.trace("param {} is {}", null, null);

		assertThat(errContent.size()).isZero();
		assertThat(outContent.toString())
				.contains("vararg {} is {}")
				.contains("param null is null");
	}

	@Test
    void traceDismissedInNonVerboseMode() {
		Logger log = new Loggers.ConsoleLogger("test", new PrintStream(outContent), new PrintStream(errContent), false);
		log.trace("foo");
		log.trace("foo", new IllegalArgumentException("foo"));
		log.trace("foo {}", "foo");

		assertThat(outContent.toString()).doesNotContain("foo");
		assertThat(errContent.toString()).doesNotContain("foo");

		assertThat(log.isTraceEnabled()).as("isTraceEnabled").isFalse();
	}

	@Test
    void isDebugEnabled() throws Exception {
		assertThat(logger.isDebugEnabled()).isTrue();
	}

	@Test
    void debug() throws Exception {
		logger.debug("message");

		assertThat(errContent.size()).isZero();
		assertThat(outContent.toString()).isEqualTo("[DEBUG] (" + Thread.currentThread().getName() + ") message\n");
	}

	@Test
    void debug1() throws Exception {
		logger.debug("message {} {} format", "with", 1);

		assertThat(errContent.size()).isZero();
		assertThat(outContent.toString()).isEqualTo("[DEBUG] (" + Thread.currentThread().getName() + ") message with 1 format\n");
	}

	@Test
    void debug2() throws Exception {
		logger.debug("with cause", CAUSE);

		assertThat(errContent.size()).isZero();
		assertThat(outContent.toString())
				.startsWith("[DEBUG] (" + Thread.currentThread().getName() + ") with cause - java.lang.IllegalStateException: cause" +
						"\njava.lang.IllegalStateException: cause\n" +
						"\tat reactor.util.ConsoleLoggerTest");
	}

	@Test
    void debugNulls() {
		logger.debug("vararg {} is {}", (Object[]) null);
		logger.debug("param {} is {}", null, null);

		assertThat(errContent.size()).isZero();
		assertThat(outContent.toString())
				.contains("vararg {} is {}")
				.contains("param null is null");
	}

	@Test
    void debugDismissedInNonVerboseMode() {
		Logger log = new Loggers.ConsoleLogger("test", new PrintStream(outContent), new PrintStream(errContent), false);
		log.debug("foo");
		log.debug("foo", new IllegalArgumentException("foo"));
		log.debug("foo {}", "foo");

		assertThat(outContent.toString()).doesNotContain("foo");
		assertThat(errContent.toString()).doesNotContain("foo");

		assertThat(log.isDebugEnabled()).as("isDebugEnabled").isFalse();
	}

	@Test
    void isInfoEnabled() throws Exception {
		assertThat(logger.isInfoEnabled()).isTrue();
	}

	@Test
    void info() throws Exception {
		logger.info("message");

		assertThat(errContent.size()).isZero();
		assertThat(outContent.toString()).isEqualTo("[ INFO] (" + Thread.currentThread().getName() + ") message\n");
	}

	@Test
    void info1() throws Exception {
		logger.info("message {} {} format", "with", 1);

		assertThat(errContent.size()).isZero();
		assertThat(outContent.toString()).isEqualTo("[ INFO] (" + Thread.currentThread().getName() + ") message with 1 format\n");
	}

	@Test
    void info2() throws Exception {
		logger.info("with cause", CAUSE);

		assertThat(errContent.size()).isZero();
		assertThat(outContent.toString())
				.startsWith("[ INFO] (" + Thread.currentThread().getName() + ") with cause - java.lang.IllegalStateException: cause" +
						"\njava.lang.IllegalStateException: cause\n" +
						"\tat reactor.util.ConsoleLoggerTest");
	}

	@Test
    void infoNulls() {
		logger.info("vararg {} is {}", (Object[]) null);
		logger.info("param {} is {}", null, null);

		assertThat(errContent.size()).isZero();
		assertThat(outContent.toString())
				.contains("vararg {} is {}")
				.contains("param null is null");
	}

	@Test
    void isWarnEnabled() throws Exception {
		assertThat(logger.isWarnEnabled()).isTrue();
	}

	@Test
    void warn() throws Exception {
		logger.warn("message");

		assertThat(outContent.size()).isZero();
		assertThat(errContent.toString()).isEqualTo("[ WARN] (" + Thread.currentThread().getName() + ") message\n");
	}

	@Test
    void warn1() throws Exception {
		logger.warn("message {} {} format", "with", 1);

		assertThat(outContent.size()).isZero();
		assertThat(errContent.toString()).isEqualTo("[ WARN] (" + Thread.currentThread().getName() + ") message with 1 format\n");
	}

	@Test
    void warn2() throws Exception {
		logger.warn("with cause", CAUSE);


		assertThat(outContent.size()).isZero();
		assertThat(errContent.toString())
				.startsWith("[ WARN] (" + Thread.currentThread().getName() + ") with cause - java.lang.IllegalStateException: cause" +
						"\njava.lang.IllegalStateException: cause\n" +
						"\tat reactor.util.ConsoleLoggerTest");
	}

	@Test
    void warnNulls() {
		logger.warn("vararg {} is {}", (Object[]) null);
		logger.warn("param {} is {}", null, null);

		assertThat(errContent.toString())
				.contains("vararg {} is {}")
				.contains("param null is null");
		assertThat(outContent.size()).isZero();
	}

	@Test
    void isErrorEnabled() throws Exception {
		assertThat(logger.isErrorEnabled()).isTrue();
	}

	@Test
    void error() throws Exception {
		logger.error("message");

		assertThat(outContent.size()).isZero();
		assertThat(errContent.toString()).isEqualTo("[ERROR] (" + Thread.currentThread().getName() + ") message\n");
	}

	@Test
    void error1() throws Exception {
		logger.error("message {} {} format", "with", 1);

		assertThat(outContent.size()).isZero();
		assertThat(errContent.toString()).isEqualTo("[ERROR] (" + Thread.currentThread().getName() + ") message with 1 format\n");
	}

	@Test
    void error2() throws Exception {
		logger.error("with cause", CAUSE);

		assertThat(outContent.size()).isZero();
		assertThat(errContent.toString())
				.startsWith("[ERROR] (" + Thread.currentThread().getName() + ") with cause - java.lang.IllegalStateException: cause" +
						"\njava.lang.IllegalStateException: cause\n" +
						"\tat reactor.util.ConsoleLoggerTest");
	}

	@Test
    void errorNulls() {
		logger.error("vararg {} is {}", (Object[]) null);
		logger.error("param {} is {}", null, null);

		assertThat(errContent.toString())
				.contains("vararg {} is {}")
				.contains("param null is null");
		assertThat(outContent.size()).isZero();
	}

	@Test
    void formatNull() {
		logger.info(null, null, null);

		assertThat(errContent.size()).isZero();
		assertThat(outContent.toString())
				.isEqualTo("[ INFO] (" + Thread.currentThread().getName() + ") null\n");
	}

}
