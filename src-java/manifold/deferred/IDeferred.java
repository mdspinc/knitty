package manifold.deferred;

public interface IDeferred {
   Object executor();

   boolean realized();

   Object onRealized(Object var1, Object var2);

   Object successValue(Object var1);

   Object errorValue(Object var1);
}
