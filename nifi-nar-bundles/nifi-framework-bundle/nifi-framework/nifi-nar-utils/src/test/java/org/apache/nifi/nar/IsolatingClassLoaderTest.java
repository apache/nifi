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
package org.apache.nifi.nar;

import org.apache.nifi.bundle.Bundle;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IsolatingClassLoaderTest {

    private static final URL NIFI_FRAMEWORK_API_URL = Bundle.class.getProtectionDomain().getCodeSource().getLocation();

    private static final String PROCESSOR_CLASS_NAME = "org.apache.nifi.processor.Processor";
    private static final String BUNDLE_CLASS_NAME = "org.apache.nifi.bundle.Bundle";

    private static final ClassLoader SYSTEM_CLASS_LOADER = ClassLoader.getSystemClassLoader();

    private IsolatingRootClassLoader isolatingRootClassLoader;

    @BeforeEach
    public void beforeEach() {
        isolatingRootClassLoader = new IsolatingRootClassLoader(SYSTEM_CLASS_LOADER);
    }

    @Test
    public void testNonIsolating() throws ClassNotFoundException {
        IsolatingClassLoader isolatingClassLoader = new IsolatingClassLoader(new URL[] {NIFI_FRAMEWORK_API_URL}, SYSTEM_CLASS_LOADER);

        assertNonIsolating(isolatingClassLoader);

        testLoadSystemClass(isolatingClassLoader);
        testLoadNonIsolatedClass(PROCESSOR_CLASS_NAME, isolatingClassLoader);
        testLoadNonIsolatedClass(BUNDLE_CLASS_NAME, isolatingClassLoader);
    }

    @Test
    public void testIsolatingLevel1() throws ClassNotFoundException {
        IsolatingClassLoader isolatingClassLoader = new IsolatingClassLoader(new URL[] {NIFI_FRAMEWORK_API_URL}, isolatingRootClassLoader);

        assertIsolating(isolatingClassLoader);

        testLoadSystemClass(isolatingClassLoader);
        testLoadNonIsolatedClass(PROCESSOR_CLASS_NAME, isolatingClassLoader);
        testLoadIsolatedClass(BUNDLE_CLASS_NAME, isolatingClassLoader);
    }

    @Test
    public void testIsolatingLevel2() throws ClassNotFoundException {
        IsolatingClassLoader isolatingClassLoader1 = new IsolatingClassLoader(new URL[0], isolatingRootClassLoader);
        IsolatingClassLoader isolatingClassLoader2 = new IsolatingClassLoader(new URL[] {NIFI_FRAMEWORK_API_URL}, isolatingClassLoader1);

        assertIsolating(isolatingClassLoader2);

        testLoadSystemClass(isolatingClassLoader2);
        testLoadNonIsolatedClass(PROCESSOR_CLASS_NAME, isolatingClassLoader2);
        testLoadIsolatedClass(BUNDLE_CLASS_NAME, isolatingClassLoader2);
    }

    @Test
    public void testIsolatingLevel1AndLevel2() throws ClassNotFoundException {
        IsolatingClassLoader isolatingClassLoader1 = new IsolatingClassLoader(new URL[] {NIFI_FRAMEWORK_API_URL}, isolatingRootClassLoader);
        IsolatingClassLoader isolatingClassLoader2 = new IsolatingClassLoader(new URL[] {NIFI_FRAMEWORK_API_URL}, isolatingClassLoader1);

        assertIsolating(isolatingClassLoader1);
        assertIsolating(isolatingClassLoader2);

        testLoadSystemClass(isolatingClassLoader1);
        testLoadSystemClass(isolatingClassLoader2);
        testLoadNonIsolatedClass(PROCESSOR_CLASS_NAME, isolatingClassLoader1);
        testLoadNonIsolatedClass(PROCESSOR_CLASS_NAME, isolatingClassLoader2);
        testLoadIsolatedClass(BUNDLE_CLASS_NAME, isolatingClassLoader1);
        testLoadIsolatedClass(BUNDLE_CLASS_NAME, isolatingClassLoader2, isolatingClassLoader1);
    }

    private void assertNonIsolating(IsolatingClassLoader isolatingClassLoader) {
        assertFalse(isolatingClassLoader.isIsolating());
        assertNull(isolatingClassLoader.getRootClassLoader());
    }

    private void assertIsolating(IsolatingClassLoader isolatingClassLoader) {
        assertTrue(isolatingClassLoader.isIsolating());
        assertSame(isolatingRootClassLoader, isolatingClassLoader.getRootClassLoader());
    }

    private void testLoadSystemClass(ClassLoader classLoader) throws ClassNotFoundException {
        assertNotNull(classLoader.loadClass("java.lang.Object"));
    }

    private void testLoadNonIsolatedClass(String className, IsolatingClassLoader isolatingClassLoader) throws ClassNotFoundException {
        Class<?> clazz = isolatingClassLoader.loadClass(className);
        assertSame(SYSTEM_CLASS_LOADER, clazz.getClassLoader());
    }

    private void testLoadIsolatedClass(String className, IsolatingClassLoader isolatingClassLoader) throws ClassNotFoundException {
        testLoadIsolatedClass(className, isolatingClassLoader, isolatingClassLoader);
    }

    private void testLoadIsolatedClass(String className, IsolatingClassLoader isolatingClassLoader, IsolatingClassLoader expectedClassLoader) throws ClassNotFoundException {
        Class<?> clazz = isolatingClassLoader.loadClass(className);
        assertSame(expectedClassLoader, clazz.getClassLoader());
    }

}
