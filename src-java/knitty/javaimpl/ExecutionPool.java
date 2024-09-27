package knitty.javaimpl;

import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

import clojure.lang.AFn;
import clojure.lang.Var;

public abstract class ExecutionPool {

    public abstract void fork(AFn fn);
    public abstract void run(AFn fn);

    private ExecutionPool() {}

    public static ExecutionPool adapt(Executor executor, Object bframe) {
        if (executor instanceof ForkJoinPool) {
            return new ForkJoinPoolPool((ForkJoinPool) executor, bframe);
        } else if (executor != null) {
            return new ExecutorPool(executor, bframe);
        } else {
            return new DirectCallPool(bframe);
        }
    }

    static final class DirectCallPool extends ExecutionPool {

        private final Object bframe;

        public DirectCallPool(Object bframe) {
            this.bframe = bframe;
        }

        public void fork(AFn fn) {
            fn.invoke();
        }

        public void run(AFn fn) {
            Object oldf = pushBFrame(bframe);
            try {
                fn.invoke();
            } finally {
                popBFrame(oldf);
            }
        }
    }

    static final class ExecutorPool extends ExecutionPool {

        private final class FnWrapper implements Runnable {
            private final AFn fn;

            private FnWrapper(AFn fn) {
                this.fn = fn;
            }

            public void run() {
                insideExecutor.set(true);
                Object oldf = pushBFrame(bframe);
                try {
                    fn.invoke();
                } finally {
                    popBFrame(oldf);
                    insideExecutor.set(false);
                }
            }
        }

        private final Executor executor;
        private final Object bframe;
        private final ThreadLocal<Boolean> insideExecutor = new ThreadLocal<>();

        public ExecutorPool(Executor executor, Object bframe) {
            this.executor = executor;
            this.bframe = bframe;
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
                Object oldf = pushBFrame(bframe);
                try {
                    fn.invoke();
                } catch (Throwable e) {
                    KDeferred.logError(e, "uncaugh exception in fj-task");
                } finally {
                    popBFrame(oldf);
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
        private final Object bframe;

        public ForkJoinPoolPool(ForkJoinPool pool, Object bframe) {
            this.pool = pool;
            this.bframe = bframe;
        }

        public void fork(AFn fn) {
            new FnForkTask(fn).fork(this.pool);
        }

        public void run(AFn fn) {
            this.pool.execute(new FnForkTask(fn));
        }
    }

    private static Object pushBFrame(Object bframe) {
        if (bframe == null) {
            return null;
        }
        Object frame = Var.getThreadBindingFrame();
        Var.resetThreadBindingFrame(bframe);
        return frame;
    }

    private static void popBFrame(Object bframe) {
        if (bframe != null) {
            Var.resetThreadBindingFrame(bframe);
        }
    }
}
