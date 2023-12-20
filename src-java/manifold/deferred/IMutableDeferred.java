package manifold.deferred;

public interface IMutableDeferred {
   Object success(Object var1);

   Object success(Object var1, Object var2);

   Object error(Object var1);

   Object error(Object var1, Object var2);

   Object claim();

   Object addListener(Object var1);

   Object cancelListener(Object var1);
}
