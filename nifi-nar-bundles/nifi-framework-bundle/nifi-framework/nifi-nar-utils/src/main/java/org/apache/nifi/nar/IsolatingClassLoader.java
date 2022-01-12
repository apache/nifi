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

import java.net.URL;
import java.net.URLClassLoader;

/**
 * Class loader that can be used to load classes in isolated way.
 * <p>
 * It may or may not be used together with {@link IsolatingRootClassLoader}. When {@link IsolatingRootClassLoader} is not present in the class loader hierarchy, then {@code IsolatingClassLoader}
 * works as a regular class loader and applies parent-first approach.
 * <p>
 * When {@code IsolatingClassLoader} is a descendant of an {@link IsolatingRootClassLoader} (more precisely: a descendant, plus all ancestor class loaders up to the isolating root are also of type
 * {@code IsolatingClassLoader}), then isolating mode is turned on. In this case, {@code IsolatingClassLoader} tries to load the requested class from the isolated path first (kind of a child-first
 * approach in terms of the isolated branch up to the isolating root). If it succeeds, the class is returned. If it fails, then {@code IsolatingClassLoader} falls back on
 * {@link IsolatingRootClassLoader#loadClassNonIsolated(String, boolean)} which forwards the request to the higher level class loaders.
 * <p>
 * {@code IsolatingClassLoader} can be used as a super class of another custom class loader or can be instantiated directly.
 */
public class IsolatingClassLoader extends URLClassLoader {

    private final IsolatingRootClassLoader rootClassLoader;

    public IsolatingClassLoader(URL[] urls) {
        super(urls);

        rootClassLoader = null;
    }

    public IsolatingClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);

        if (parent instanceof IsolatingRootClassLoader) {
            rootClassLoader = (IsolatingRootClassLoader) parent;
        } else if (parent instanceof IsolatingClassLoader) {
            rootClassLoader = ((IsolatingClassLoader) parent).getRootClassLoader();
        } else {
            rootClassLoader = null;
        }
    }

    public boolean isIsolating() {
        return rootClassLoader != null;
    }

    public IsolatingRootClassLoader getRootClassLoader() {
        return rootClassLoader;
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        if (rootClassLoader == null) {
            return super.loadClass(name);
        } else {
            try {
                // try to load it first
                return loadClass(name, false);
            } catch (ClassNotFoundException e) {
                // fall back on the root class loader's parent
                return rootClassLoader.loadClassNonIsolated(name, false);
            }
        }
    }

}
