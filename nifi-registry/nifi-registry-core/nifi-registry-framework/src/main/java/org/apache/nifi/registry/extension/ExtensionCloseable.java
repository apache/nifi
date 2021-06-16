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
package org.apache.nifi.registry.extension;

import java.io.Closeable;
import java.io.IOException;

public class ExtensionCloseable implements Closeable {
    private final ClassLoader toSet;

    private ExtensionCloseable(ClassLoader toSet) {
        this.toSet = toSet;
    }

    public static ExtensionCloseable withComponentClassLoader(final ExtensionManager manager, final Class componentClass) {

        final ClassLoader current = Thread.currentThread().getContextClassLoader();
        final ExtensionCloseable closeable = new ExtensionCloseable(current);

        ClassLoader componentClassLoader = manager.getExtensionClassLoader(componentClass.getName());
        if (componentClassLoader == null) {
            componentClassLoader = componentClass.getClassLoader();
        }

        Thread.currentThread().setContextClassLoader(componentClassLoader);
        return closeable;
    }

    public static ExtensionCloseable withClassLoader(final ClassLoader componentClassLoader) {
        final ClassLoader current = Thread.currentThread().getContextClassLoader();
        final ExtensionCloseable closeable = new ExtensionCloseable(current);
        Thread.currentThread().setContextClassLoader(componentClassLoader);
        return closeable;
    }

    @Override
    public void close() throws IOException {
        if (toSet != null) {
            Thread.currentThread().setContextClassLoader(toSet);
        }
    }
}
