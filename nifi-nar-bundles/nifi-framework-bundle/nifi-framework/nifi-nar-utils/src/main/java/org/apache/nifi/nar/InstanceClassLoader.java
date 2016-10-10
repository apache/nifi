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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * A ClassLoader created for an instance of a component which lets a client add resources to an intermediary ClassLoader
 * that will be checked first when loading/finding classes.
 *
 * Typically an instance of this ClassLoader will be created by passing in the URLs and parent from a NARClassLoader in
 * order to create a copy of the NARClassLoader without modifying it.
 */
public class InstanceClassLoader extends URLClassLoader {

    private static final Logger logger = LoggerFactory.getLogger(InstanceClassLoader.class);

    private final String identifier;
    private ShimClassLoader shimClassLoader;

    /**
     * @param identifier the id of the component this ClassLoader was created for
     * @param urls the URLs for the ClassLoader
     * @param parent the parent ClassLoader
     */
    public InstanceClassLoader(final String identifier, final URL[] urls, final ClassLoader parent) {
        super(urls, parent);
        this.identifier = identifier;
    }

    /**
     * Initializes a new ShimClassLoader for the provided resources, closing the previous ShimClassLoader if one existed.
     *
     * @param urls the URLs for the ShimClassLoader
     * @throws IOException if the previous ShimClassLoader existed and couldn't be closed
     */
    public synchronized void setInstanceResources(final URL[] urls) {
        if (shimClassLoader != null) {
            try {
                shimClassLoader.close();
            } catch (IOException e) {
                logger.warn("Unable to close URLClassLoader for " + identifier);
            }
        }

        // don't set a parent here b/c otherwise it will create an infinite loop
        shimClassLoader = new ShimClassLoader(urls, null);
    }

    /**
     * @return the URLs for the instance resources that have been set
     */
    public synchronized URL[] getInstanceResources() {
        if (shimClassLoader != null) {
            return shimClassLoader.getURLs();
        }
        return new URL[0];
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        return this.loadClass(name, false);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        Class<?> c = null;
        // first try the shim
        if (shimClassLoader != null) {
            try {
                c = shimClassLoader.loadClass(name, resolve);
            } catch (ClassNotFoundException cnf) {
                c = null;
            }
        }
        // if it wasn't in the shim try our self
        if (c == null) {
            return super.loadClass(name, resolve);
        } else {
            return c;
        }
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        Class<?> c = null;
        // first try the shim
        if (shimClassLoader != null) {
            try {
                c = shimClassLoader.findClass(name);
            } catch (ClassNotFoundException cnf) {
                c = null;
            }
        }
        // if it wasn't in the shim try our self
        if (c == null) {
            return super.findClass(name);
        } else {
            return c;
        }
    }

    /**
     * Extend URLClassLoader to increase visibility of protected methods so that InstanceClassLoader can delegate.
     */
    private static class ShimClassLoader extends URLClassLoader {

        public ShimClassLoader(URL[] urls, ClassLoader parent) {
            super(urls, parent);
        }

        public ShimClassLoader(URL[] urls) {
            super(urls);
        }

        @Override
        public Class<?> findClass(String name) throws ClassNotFoundException {
            return super.findClass(name);
        }

        @Override
        public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            return super.loadClass(name, resolve);
        }

    }

}
