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

package reactor.core.scheduler;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * A runnable task for {@link Scheduler} Workers that can run periodically
 **/
final class PeriodicWorkerTask extends PeriodicTask {

	static final Composite DISPOSED = new EmptyCompositeDisposable();

	volatile Composite parent;
	static final AtomicReferenceFieldUpdater<PeriodicWorkerTask, Composite> PARENT =
			AtomicReferenceFieldUpdater.newUpdater(PeriodicWorkerTask.class, Composite.class, "parent");

	PeriodicWorkerTask(Runnable task, Composite parent) {
		super(task);
		PARENT.lazySet(this, parent);
	}

	@Override
	public void dispose() {
		super.dispose();

		for (;;) {
			Composite o = parent;
			if (o == DISPOSED || o == null) {
				return;
			}
			if (PARENT.compareAndSet(this, o, DISPOSED)) {
				o.remove(this);
				return;
			}
		}
	}
}
