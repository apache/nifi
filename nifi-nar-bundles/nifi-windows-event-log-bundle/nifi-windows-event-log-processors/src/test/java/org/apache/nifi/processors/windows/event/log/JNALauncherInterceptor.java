/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.windows.event.log;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import com.sun.jna.Native;
import com.sun.jna.platform.win32.Kernel32Util;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import org.junit.platform.launcher.LauncherInterceptor;

import static org.junit.jupiter.api.Assertions.fail;

public class JNALauncherInterceptor implements LauncherInterceptor {
    public static final String NATIVE_CANONICAL_NAME = Native.class.getCanonicalName();
    public static final String LOAD_LIBRARY = "loadLibrary";
    public static final String TEST_COMPUTER_NAME = "testComputerName";
    public static final String KERNEL_32_UTIL_CANONICAL_NAME = Kernel32Util.class.getCanonicalName();

    private final URLClassLoader jnaMockClassLoader;

    public JNALauncherInterceptor() {
        Map<String, Map<String, String>> classOverrideMap = getClassOverrideMap();
        String classpath = System.getProperty("java.class.path");
        URL[] result = Pattern.compile(File.pathSeparator).splitAsStream(classpath).map(Paths::get).map(Path::toAbsolutePath).map(Path::toUri)
                .map(uri -> {
                    URL url = null;
                    try {
                        url = uri.toURL();
                    } catch (MalformedURLException e) {
                        fail(String.format("Unable to create URL for classpath entry '%s'", uri));
                    }
                    return url;
                })
                .toArray(URL[]::new);
        jnaMockClassLoader = new URLClassLoader(result, null) {
            @Override
            protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                Map<String, String> classOverrides = classOverrideMap.get(name);
                if (classOverrides != null) {
                    ClassPool classPool = ClassPool.getDefault();
                    try {
                        CtClass ctClass = classPool.get(name);
                        try {
                            for (Map.Entry<String, String> methodAndBody : classOverrides.entrySet()) {
                                for (CtMethod loadLibrary : ctClass.getDeclaredMethods(methodAndBody.getKey())) {
                                    loadLibrary.setBody(methodAndBody.getValue());
                                }
                            }

                            byte[] bytes = ctClass.toBytecode();
                            Class<?> definedClass = defineClass(name, bytes, 0, bytes.length);
                            if (resolve) {
                                resolveClass(definedClass);
                            }
                            return definedClass;
                        } finally {
                            ctClass.detach();
                        }
                    } catch (Exception e) {
                        throw new ClassNotFoundException(name, e);
                    }
                } else if (name.startsWith("org.junit.")) {
                    Class<?> result = JNALauncherInterceptor.class.getClassLoader().loadClass(name);
                    if (resolve) {
                        resolveClass(result);
                    }
                    return result;
                }
                return super.loadClass(name, resolve);
            }
        };
    }

    @Override
    public <T> T intercept(final Invocation<T> invocation) {
        final Thread currentThread = Thread.currentThread();
        final ClassLoader originalClassLoader = currentThread.getContextClassLoader();
        currentThread.setContextClassLoader(jnaMockClassLoader);
        try {
            return invocation.proceed();
        } finally {
            currentThread.setContextClassLoader(originalClassLoader);
        }
    }

    @Override
    public void close() {
        try {
            jnaMockClassLoader.close();
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to close custom class loader", e);
        }
    }

    private Map<String, Map<String, String>> getClassOverrideMap() {
        final Map<String, Map<String, String>> classOverrideMap = new HashMap<>();

        final Map<String, String> nativeOverrideMap = new HashMap<>();
        nativeOverrideMap.put(LOAD_LIBRARY, "return null;");
        classOverrideMap.put(NATIVE_CANONICAL_NAME, nativeOverrideMap);

        final Map<String, String> kernel32UtilMap = new HashMap<>();
        kernel32UtilMap.put("getComputerName", "return \"" + TEST_COMPUTER_NAME + "\";");
        classOverrideMap.put(KERNEL_32_UTIL_CANONICAL_NAME, kernel32UtilMap);

        return classOverrideMap;
    }
}
