package knitty.java;

import java.io.IOException;
import java.net.URL;

import clojure.lang.DynamicClassLoader;

public class KnittyLoader extends DynamicClassLoader {

    private KnittyLoader() {
        super(KnittyLoader.class.getClassLoader());
    }

    public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (name.startsWith("knitty.java.")) {
            URL r = this.getResource(name.replace(".", "/") + ".class");
            if (r != null) {
                // Load Knitty java class as clj dynamic classes
                try {
                    byte[] b = r.openStream().readAllBytes();
                    return this.defineClass(name, b, this);
                } catch (IOException e) {
                    throw new ClassNotFoundException("unable to load class", e);
                }
            }
        }
        return super.loadClass(name, resolve);
    }

    public static void init() throws ClassNotFoundException, IOException {
        KnittyLoader cl = new KnittyLoader();
        cl.loadKnittyClasses();
        cl.close();
    }

    void loadKnittyClasses() throws ClassNotFoundException {
        loadClass("knitty.java.KaList");
        loadClass("knitty.java.KaDeferred");
        loadClass("knitty.java.MDM");
    }
}
