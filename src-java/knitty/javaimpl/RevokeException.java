package knitty.javaimpl;

public class RevokeException extends RuntimeException {

    public static final RevokeException INSTANCE = new RevokeException();

    public RevokeException() {
        super("deferred is revoked", null, false, false);
    }

    public RevokeException(Throwable cause) {
        super("deferred is revoked", cause, false, false);
    }

    public synchronized Throwable fillInStackTrace() {
        return this;
   }
}
