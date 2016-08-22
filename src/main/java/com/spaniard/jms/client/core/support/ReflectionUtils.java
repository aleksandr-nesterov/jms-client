package com.spaniard.jms.client.core.support;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import static java.lang.reflect.Modifier.*;

/**
 * created 10.09.15
 */
public abstract class ReflectionUtils {

    private ReflectionUtils() {
    }

    public static Set<Class> getAllClassesInPackage(String packageName) {
        List<Class> classes = new ArrayList<>();
        URL resource = Thread.currentThread().getContextClassLoader().getResource(packageName.replace('.', '/'));
        if (resource == null) {
            return Collections.emptySet();
        }
        resource.getPath();
        if (resource.toString().startsWith("jar:")) {
            processJarfile(resource, packageName, classes);
        } else {
            processDirectory(new File(resource.getPath()), packageName, classes);
        }

        // remove interfaces, inner static or abstract classes
        for (Iterator<Class> it = classes.iterator(); it.hasNext(); ) {
            Class modelClass = it.next();
            int clsModifiers = modelClass.getModifiers();
            if (isInterface(clsModifiers) || isAbstract(clsModifiers) || isStatic(clsModifiers)) {
                it.remove();
            }
        }

        return Collections.unmodifiableSet(new HashSet<>(classes));
    }

    private static Class<?> loadClass(String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Unexpected ClassNotFoundException loading class '" + className + "'");
        }
    }

    private static void processDirectory(File directory, String packageName, List<Class> classes) {
        String[] files = directory.list();
        for (int i = 0; i < files.length; i++) {
            String fileName = files[i];
            String className = null;
            if (fileName.endsWith(".class")) {
                className = packageName + '.' + fileName.substring(0, fileName.length() - 6);
            }
            if (className != null) {
                classes.add(loadClass(className));
            }
            File subdir = new File(directory, fileName);
            if (subdir.isDirectory()) {
                processDirectory(subdir, packageName + '.' + fileName, classes);
            }
        }
    }

    private static void processJarfile(URL resource, String packageName, List<Class> classes) {
        String relPath = packageName.replace('.', '/');
        String resPath = resource.getPath();
        String jarPath = resPath.replaceFirst("[.]jar[!].*", ".jar").replaceFirst("file:", "");
        JarFile jarFile;
        try {
            jarFile = new JarFile(jarPath);
        } catch (IOException e) {
            throw new RuntimeException("Unexpected IOException reading JAR File '" + jarPath + "'", e);
        }
        Enumeration<JarEntry> entries = jarFile.entries();
        while (entries.hasMoreElements()) {
            JarEntry entry = entries.nextElement();
            String entryName = entry.getName();
            String className = null;
            if (entryName.endsWith(".class") && entryName.startsWith(relPath) && entryName.length() > (relPath.length() + "/".length())) {
                className = entryName.replace('/', '.').replace('\\', '.').replace(".class", "");
            }
            if (className != null) {
                classes.add(loadClass(className));
            }
        }
    }

}