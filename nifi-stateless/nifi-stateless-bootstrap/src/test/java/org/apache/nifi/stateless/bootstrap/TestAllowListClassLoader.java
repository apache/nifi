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

package org.apache.nifi.stateless.bootstrap;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TestAllowListClassLoader {

    @Test
    void testIsModuleAllowedNullReturnsFalse() {
        final AllowListClassLoader classLoader = new AllowListClassLoader(
                ClassLoader.getSystemClassLoader(),
                Collections.emptySet()
        );

        // Before fix: moduleName.startsWith(...) threw NullPointerException
        // After fix: returns false safely
        final boolean allowed = classLoader.isModuleAllowed(null);
        assertFalse(allowed, "A null module name should not be allowed");
    }

    @Test
    void testLoadClassFromAllowedJavaModule() throws ClassNotFoundException {
        final AllowListClassLoader classLoader = new AllowListClassLoader(
                ClassLoader.getSystemClassLoader(),
                Collections.emptySet()
        );

        final Class<?> loadedClass = classLoader.loadClass("java.lang.String");
        assertNotNull(loadedClass);
        assertEquals(String.class, loadedClass);
    }

    @Test
    void testLoadExplicitlyAllowedClass() throws ClassNotFoundException {
        final String explicitClassName = TestAllowListClassLoader.class.getName();
        final Set<String> allowedClassNames = Collections.singleton(explicitClassName);

        final AllowListClassLoader classLoader = new AllowListClassLoader(
                ClassLoader.getSystemClassLoader(),
                allowedClassNames
        );

        final Class<?> loadedClass = classLoader.loadClass(explicitClassName);
        assertNotNull(loadedClass);
        assertEquals(TestAllowListClassLoader.class, loadedClass);
    }
}
