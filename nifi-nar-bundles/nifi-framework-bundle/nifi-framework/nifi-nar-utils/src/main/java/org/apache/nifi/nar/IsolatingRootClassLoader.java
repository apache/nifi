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
 * Class loader that does not have any class path and does not forward class loading requests to its parent. Therefore, it can be used to isolate the descendant class loaders from the ancestors
 * in the class loader hierarchy.
 * It is needed when NiFi is running embedded in a hosting environment which has its own class loading mechanism and shared jars on the system class path (e.g. NiFi Stateless in Kafka Connect).
 * <p>
 * {@code IsolatingRootClassLoader} can work in tandem with {@link IsolatingClassLoader} where {@code IsolatingRootClassLoader} is the root of the subtree to be isolated
 * and all descendant class loaders are of type {@link IsolatingClassLoader}.
 * <p>
 * {@code IsolatingRootClassLoader} also provides a method to access the embedding class loader hierarchy and to load a class from there ({@link #loadClassNonIsolated(String, boolean)}).
 * Descendant class loaders are expected to call it when a class can not be found in the isolated subtree (e.g. core java classes, like {@code java.lang.Object}).
 */
public class IsolatingRootClassLoader extends URLClassLoader {

    public IsolatingRootClassLoader(ClassLoader parent) {
        super(new URL[0], parent);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        return null;
    }

    protected Class<?> loadClassNonIsolated(String name, boolean resolve) throws ClassNotFoundException {
        return super.loadClass(name, resolve);
    }

}
