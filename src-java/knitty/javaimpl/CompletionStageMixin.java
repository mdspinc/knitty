package knitty.javaimpl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import clojure.lang.ExceptionInfo;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

@SuppressWarnings({"rawtypes", "unchecked"})
public interface CompletionStageMixin extends CompletionStage {

    void consume(Consumer x, Consumer e, Executor ex);
    CompletionStageMixin coerce(CompletionStage x);
    CompletionStageMixin dup();
    void fireValue(Object x);
    void fireError(Object x);
    Executor defaultExecutor();

    class Utils {

        private Utils() {}

        static CompletionStageMixin fmap(CompletionStageMixin that, Function fx, Function fe, Executor ex) {
            CompletionStageMixin d = that.dup();
            that.consume(
                (x) -> {
                    try {
                        d.fireValue(fx.apply(x));
                    } catch (Throwable t) {
                        d.fireError(t);
                    }
                },
                (e) -> {
                    if (fe == null) {
                        d.fireError(e);
                    } else {
                        try {
                            d.fireValue(fe.apply(e));
                        } catch (Throwable t) {
                            t.addSuppressed((Throwable) (e));
                            d.fireError(t);
                        }
                    }
                },
                ex);
            return d;
        }

        static Consumer failCallback(CompletionStageMixin d, Executor ex) {
            return  e -> { ExecutionPool.adapt(ex).fork(() -> { d.fireError(e); }); };
        }

        static CompletionStageMixin either(CompletionStageMixin a, CompletionStage b) {
            CompletionStageMixin d = a.dup();
            a.consume(d::fireValue, d::fireError, null);
            a.coerce(b).consume(d::fireValue, d::fireError, null);
            return d;
        }

        static Throwable coerceError(Object err) {
            if (err instanceof Throwable) {
                return (Throwable) err;
            } else {
                return new ExceptionInfo(
                    "invalid error object",
                        PersistentArrayMap.EMPTY.assoc(Keyword.find("error"), err)
                );
            }
        }
    }

    @Override
    default CompletionStage handle(BiFunction bf) {
        return Utils.fmap(
            this,
            x -> bf.apply(x, null),
            e -> bf.apply(null, e),
            null);
    }

    @Override
    default CompletionStage handleAsync(BiFunction bf) {
        return handleAsync(bf, defaultExecutor());
    }

    @Override
    default CompletionStage handleAsync(BiFunction bf, Executor ex) {
        return Utils.fmap(
            this,
            x -> bf.apply(x, null),
            e -> bf.apply(null, e),
            ex);
    }

    @Override
    default CompletionStage acceptEither(CompletionStage that, Consumer c) {
        return Utils.either(this, that).thenAccept(c);
    }

    @Override
    default CompletionStage acceptEitherAsync(CompletionStage that, Consumer c) {
        return acceptEitherAsync(that, c, defaultExecutor());
    }

    @Override
    default CompletionStage acceptEitherAsync(CompletionStage that, Consumer c, Executor ex) {
        return Utils.either(this, that).thenAcceptAsync(c, ex);
    }

    @Override
    default CompletionStage applyToEither(CompletionStage that, Function f) {
        return Utils.either(this, that).thenApply(f);
    }

    @Override
    default CompletionStage applyToEitherAsync(CompletionStage that, Function f) {
        return applyToEitherAsync(that, f, defaultExecutor());
    }

    @Override
    default CompletionStage applyToEitherAsync(CompletionStage that, Function f, Executor ex) {
        return Utils.either(this, that).thenApplyAsync(f, ex);
    }

    @Override
    default CompletionStage exceptionally(Function f) {
        return Utils.fmap(this, Function.identity(), f, null);
    }

    @Override
    default CompletionStage runAfterBoth(CompletionStage that, Runnable run) {
        CompletionStageMixin d = dup();
        this.consume(
            x -> coerce(that).consume(y -> { run.run(); d.fireValue(null); }, d::fireError, null),
            d::fireError,
            null);
        return d;
    }

    @Override
    default CompletionStage runAfterBothAsync(CompletionStage that, Runnable run) {
        return runAfterBothAsync(that, run, defaultExecutor());
    }

    @Override
    default CompletionStage runAfterBothAsync(CompletionStage that, Runnable run, Executor ex) {
        CompletionStageMixin d = dup();
        this.consume(
            x -> coerce(that).consume(y -> { run.run(); d.fireValue(null); }, d::fireError, ex),
            Utils.failCallback(d, ex),
            null);
        return d;
    }

    @Override
    default CompletionStage runAfterEither(CompletionStage that, Runnable run) {
        return Utils.either(this, that).thenRun(run);
    }

    @Override
    default CompletionStage runAfterEitherAsync(CompletionStage that, Runnable run) {
        return runAfterEitherAsync(that, run, defaultExecutor());
    }

    @Override
    default CompletionStage runAfterEitherAsync(CompletionStage that, Runnable run, Executor ex) {
        return Utils.either(this, that).thenRunAsync(run, ex);
    }

    @Override
    default CompletionStage thenAccept(Consumer c) {
        return Utils.fmap(
            this,
            x -> { c.accept(x); return null; },
            null,
            null);
    }

    @Override
    default CompletionStage thenAcceptAsync(Consumer c) {
        return thenAcceptAsync(c, defaultExecutor());
    }

    @Override
    default CompletionStage thenAcceptAsync(Consumer c, Executor ex) {
        return Utils.fmap(
            this,
            x -> { c.accept(x); return null; }, null, ex);
    }

    @Override
    default CompletionStage thenAcceptBoth(CompletionStage that, BiConsumer c) {
        CompletionStageMixin d = dup();
        this.consume(
            x -> coerce(that).consume(y -> { c.accept(x, y); d.fireValue(null); }, d::fireError, null),
            d::fireError,
            null);
        return d;
    }

    @Override
    default CompletionStage thenAcceptBothAsync(CompletionStage that, BiConsumer c) {
        return thenAcceptBothAsync(that, c, defaultExecutor());
    }

    @Override
    default CompletionStage thenAcceptBothAsync(CompletionStage that, BiConsumer c, Executor ex) {
        CompletionStageMixin d = dup();
        this.consume(
            x -> coerce(that).consume(y -> { c.accept(x, y); d.fireValue(null); }, d::fireError, ex),
            Utils.failCallback(d, ex),
            null);
        return d;
    }

    @Override
    default CompletionStage thenApply(Function f) {
        return Utils.fmap(this, f::apply, null, null);
    }

    @Override
    default CompletionStage thenApplyAsync(Function f) {
        return thenApplyAsync(f, defaultExecutor());
    }

    @Override
    default CompletionStage thenApplyAsync(Function f, Executor ex) {
        return Utils.fmap(this, f::apply, null, ex);
    }

    @Override
    default CompletionStage thenCombine(CompletionStage that, BiFunction bf) {
        CompletionStageMixin d = dup();
        this.consume(
            x -> coerce(that).consume(y -> d.fireValue(bf.apply(x, y)), d::fireError, null),
            d::fireError,
            null);
        return d;
    }

    @Override
    default CompletionStage thenCombineAsync(CompletionStage that, BiFunction bf) {
        return thenCombineAsync(that, bf, defaultExecutor());
    }

    @Override
    default CompletionStage thenCombineAsync(CompletionStage that, BiFunction bf, Executor ex) {
        CompletionStageMixin d = dup();
        this.consume(
            x -> coerce(that).consume(y -> d.fireValue(bf.apply(x, y)), d::fireError, ex),
            Utils.failCallback(d, ex),
            null);
        return d;
    }

    @Override
    default CompletionStage thenCompose(Function f) {
        CompletionStageMixin d = dup();
        this.consume(
            x -> { coerce((CompletionStage) f.apply(x)).consume(d::fireValue, d::fireError, null); },
            d::fireError,
            null);
        return d;
    }

    @Override
    default CompletionStage thenComposeAsync(Function f) {
        return thenComposeAsync(f, defaultExecutor());
    }

    @Override
    default CompletionStage thenComposeAsync(Function f, Executor ex) {
        CompletionStageMixin d = dup();
        this.consume(
            x -> { coerce((CompletionStage) f.apply(x)).consume(d::fireValue, d::fireError, ex); },
            d::fireError,
            ex);
        return d;
    }

    @Override
    default CompletionStage thenRun(Runnable run) {
        return Utils.fmap(
            this,
            x -> { run.run(); return null; },
            null,
            null);
    }

    @Override
    default CompletionStage thenRunAsync(Runnable run) {
        return thenRunAsync(run, defaultExecutor());
    }

    @Override
    default CompletionStage thenRunAsync(Runnable run, Executor ex) {
        return Utils.fmap(
            this,
            x -> { run.run(); return null; },
            null,
            ex);
    }

    @Override
    default CompletableFuture toCompletableFuture() {
        CompletableFuture cf = new CompletableFuture<>();
        this.consume(cf::complete, e -> cf.completeExceptionally(Utils.coerceError(e)), null);
        return cf;
    }

    @Override
    default CompletionStage whenComplete(BiConsumer c) {
        CompletionStageMixin d = dup();
        this.consume(
            x -> {
                try {
                    c.accept(x, null);
                } catch (Throwable t) {
                    d.fireError(t);
                    return;
                }
                d.fireValue(x);
            },
            e -> {
                try {
                    c.accept(null, e);
                } catch (Throwable t) {
                    ((Throwable) e).addSuppressed(t);
                }
                d.fireError(e);
            },
            null);
        return d;
    }

    @Override
    default CompletionStage whenCompleteAsync(BiConsumer c) {
        return whenCompleteAsync(c, defaultExecutor());
    }

    @Override
    default CompletionStage whenCompleteAsync(BiConsumer c, Executor ex) {
        CompletionStageMixin d = dup();
        this.consume(
            x -> {
                try {
                    c.accept(x, null);
                } catch (Throwable t) {
                    d.fireError(t);
                    return;
                }
                d.fireValue(x);
            },
            e -> {
                try {
                    c.accept(null, e);
                } catch (Throwable t) {
                    ((Throwable) e).addSuppressed(t);
                }
                d.fireError(e);
            },
            ex);
        return d;
    }
}
