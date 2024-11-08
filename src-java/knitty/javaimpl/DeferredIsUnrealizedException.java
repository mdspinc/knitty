package knitty.javaimpl;

public class DeferredIsUnrealizedException extends IllegalStateException {
    public DeferredIsUnrealizedException() {
        super("deferred is not yet realized");
    }
}
