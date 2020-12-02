package reactor.core.scheduler;

import reactor.core.Disposable;

import java.util.concurrent.Callable;

public abstract class Task implements Runnable, Disposable, Callable<Void> {
    final Runnable runnableTask;

    Task(Runnable runnableTask){
        this.runnableTask = runnableTask;
    }
}
