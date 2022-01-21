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

import java.util.Collections;
import java.util.Set;

/**
 * <p>
 *     A ClassLoader that blocks a specific set of selected classes from being loaded by its parent. This ClassLoader does not load any classes itself
 *     but serves as a mechanism for preventing unwanted classes from a parent from being used.
 * </p>
 * <p>
 *     Because Stateless NiFi is designed to run in an embedded environment, the classes that are available on the "System ClassLoader" (aka App ClassLoader)
 *     cannot be determined - or prevented/controlled. However, if there are conflicts between what is in the System ClassLoader and an extension (such as bringing
 *     in different versions of a popular JSON parsing library, for instance), this can cause the extensions not to function properly.
 * </p>
 * <p>
 *     Because we cannot control what is loaded by the System ClassLoader (that's up to the embedding application), the best that we can do is to block NiFi's extensions'
 *     ClassLoaders from accessing those classes. This ClassLoader allows us to do just that, blocking specific classes that have been loaded by the parent ClassLoader
 *     from being accessible by child ClassLoaders.
 * </p>
 */
public class BlockListClassLoader extends ClassLoader {
    private final Set<String> blockList;

    public BlockListClassLoader(final ClassLoader parent, final Set<String> blockList) {
        super(parent);
        this.blockList = blockList;
    }

    /**
     * @return the set of all Class names that will be blocked from loading by the parent
     */
    public Set<String> getClassesBlocked() {
        return Collections.unmodifiableSet(blockList);
    }

    @Override
    protected Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
        if (blockList.contains(name)) {
            throw new ClassNotFoundException(name + " was blocked by BlockListClassLoader");
        }

        return super.loadClass(name, resolve);
    }

    @Override
    protected Class<?> findClass(final String name) throws ClassNotFoundException {
        if (blockList.contains(name)) {
            throw new ClassNotFoundException(name + " was blocked by BlockListClassLoader");
        }

        return super.findClass(name);
    }
}
