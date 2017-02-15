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

import java.io.Closeable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class NarCloseable implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(NarCloseable.class);

    public static NarCloseable withNarLoader() {
        final ClassLoader current = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(NarThreadContextClassLoader.getInstance());
        return new NarCloseable(current);
    }

    /**
     * Sets the current thread context class loader to the specific appropriate class loader for the given
     * component. If the component requires per-instance class loading then the class loader will be the
     * specific class loader for instance with the given identifier, otherwise the class loader will be
     * the NARClassLoader.
     *
     * @param componentClass the component class
     * @param componentIdentifier the identifier of the component
     * @return NarCloseable with the current thread context classloader jailed to the Nar
     *              or instance class loader of the component
     */
    public static NarCloseable withComponentNarLoader(final Class componentClass, final String componentIdentifier) {
        final ClassLoader current = Thread.currentThread().getContextClassLoader();

        ClassLoader componentClassLoader = ExtensionManager.getInstanceClassLoader(componentIdentifier);
        if (componentClassLoader == null) {
            componentClassLoader = componentClass.getClassLoader();
        }

        Thread.currentThread().setContextClassLoader(componentClassLoader);
        return new NarCloseable(current);
    }

    /**
     * Sets the current thread context class loader to the provided class loader, and returns a NarCloseable that will
     * return the current thread context class loader to it's previous state.
     *
     * @param componentNarLoader the class loader to set as the current thread context class loader
     *
     * @return NarCloseable that will return the current thread context class loader to its previous state
     */
    public static NarCloseable withComponentNarLoader(final ClassLoader componentNarLoader) {
        final ClassLoader current = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(componentNarLoader);
        return new NarCloseable(current);
    }

    /**
     * Creates a Closeable object that can be used to to switch to current class
     * loader to the framework class loader and will automatically set the
     * ClassLoader back to the previous class loader when closed
     *
     * @return a NarCloseable
     */
    public static NarCloseable withFrameworkNar() {
        final ClassLoader frameworkClassLoader;
        try {
            frameworkClassLoader = NarClassLoaders.getInstance().getFrameworkBundle().getClassLoader();
        } catch (final Exception e) {
            // This should never happen in a running instance, but it will occur in unit tests
            logger.error("Unable to access Framework ClassLoader due to " + e + ". Will continue without changing ClassLoaders.");
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }

            return new NarCloseable(null);
        }

        final ClassLoader current = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(frameworkClassLoader);
        return new NarCloseable(current);
    }

    private final ClassLoader toSet;

    private NarCloseable(final ClassLoader toSet) {
        this.toSet = toSet;
    }

    @Override
    public void close() {
        if (toSet != null) {
            Thread.currentThread().setContextClassLoader(toSet);
        }
    }
}
