package knitty.javaimpl;

import java.util.concurrent.CancellationException;

public class YankFinishedException extends CancellationException {

   public static final YankFinishedException INTANCE = new YankFinishedException();

   YankFinishedException() {
      super("yank is already finished");
   }

   public synchronized Throwable fillInStackTrace() {
    return this;
   }
}