package knitty.javaimpl;

import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

import clojure.lang.AFn;

public abstract class ExecutionPool {

    public abstract void fork(AFn fn);
    public abstract void run(AFn fn);

    private ExecutionPool() {}

    public static ExecutionPool adapt(Executor executor) {
        if (executor instanceof ForkJoinPool) {
            return new ForkJoinPoolPool((ForkJoinPool) executor);
        } else if (executor != null) {
            return new ExecutorPool(executor);
        } else {
            return new DirectCallPool();
        }
    }

    static final class DirectCallPool extends ExecutionPool {

        public void fork(AFn fn) {
            fn.invoke();
        }

        public void run(AFn fn) {
            fn.invoke();
        }
    }

    static final class ExecutorPool extends ExecutionPool {

        private final class FnWrapper implements Runnable {
            private final AFn fn;

            private FnWrapper(AFn fn) {
                this.fn = fn;
            }

            public void run() {
                insideExecutor.set(Boolean.TRUE);
                fn.invoke();
            }
        }

        private final Executor executor;
        private final ThreadLocal<Boolean> insideExecutor = new ThreadLocal<>();

        public ExecutorPool(Executor executor) {
            this.executor = executor;
        }

        public void fork(AFn fn) {
            this.executor.execute(new FnWrapper(fn));
        }

        public void run(AFn fn) {
            if (insideExecutor.get() != null) {
                fn.invoke();
            } else {
                this.executor.execute(new FnWrapper(fn));
            }
        }
    }

    static final class ForkJoinPoolPool extends ExecutionPool {

        public class FnForkTask extends ForkJoinTask<Void> {

            private final AFn fn;

            public FnForkTask(AFn fn) {
                this.fn = fn;
            }

            protected boolean exec() {
                try {
                    fn.invoke();
                } catch (Throwable e) {
                    KDeferred.logError(e, "uncaugh exception in fj-task");
                }
                return true;
            }

            public Void getRawResult() {
                return null;
            }

            protected void setRawResult(Void value) {
            }

            public final FnForkTask fork(ForkJoinPool pool) {
                if (getPool() == pool) {
                    this.fork();
                } else {
                    pool.execute(this);
                }
                return this;
            }
        }

        private final ForkJoinPool pool;

        public ForkJoinPoolPool(ForkJoinPool pool ) {
            this.pool = pool;
        }

        public void fork(AFn fn) {
            new FnForkTask(fn).fork(this.pool);
        }

        public void run(AFn fn) {
            this.pool.execute(new FnForkTask(fn));
        }

    }
}
