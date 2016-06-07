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

import com.sun.jna.Native;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.model.InitializationError;
import org.mockito.runners.MockitoJUnitRunner;

import java.net.URLClassLoader;
import java.util.Map;

/**
 * Can't even use the JNA interface classes if the native library won't load.  This is a workaround to allow mocking them for unit tests.
 */
public abstract class JNAOverridingJUnitRunner extends Runner {
    public static final String NATIVE_CANONICAL_NAME = Native.class.getCanonicalName();
    public static final String LOAD_LIBRARY = "loadLibrary";
    private final Runner delegate;

    public JNAOverridingJUnitRunner(Class<?> klass) throws InitializationError {
        Map<String, Map<String, String>> classOverrideMap = getClassOverrideMap();
        ClassLoader jnaMockClassloader = new URLClassLoader(((URLClassLoader) JNAOverridingJUnitRunner.class.getClassLoader()).getURLs(), null) {
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
                    Class<?> result = JNAOverridingJUnitRunner.class.getClassLoader().loadClass(name);
                    if (resolve) {
                        resolveClass(result);
                    }
                    return result;
                }
                return super.loadClass(name, resolve);
            }
        };
        try {
            delegate = (Runner) jnaMockClassloader.loadClass(MockitoJUnitRunner.class.getCanonicalName()).getConstructor(Class.class)
                    .newInstance(jnaMockClassloader.loadClass(klass.getCanonicalName()));
        } catch (Exception e) {
            throw new InitializationError(e);
        }
    }

    protected abstract Map<String, Map<String, String>> getClassOverrideMap();

    @Override
    public Description getDescription() {
        return delegate.getDescription();
    }

    @Override
    public void run(RunNotifier notifier) {
        delegate.run(notifier);
    }
}
