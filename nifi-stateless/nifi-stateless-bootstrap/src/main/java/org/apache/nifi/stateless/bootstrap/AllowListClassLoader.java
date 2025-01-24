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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * <p>
 *     A ClassLoader that allows only a specific set of selected classes to be loaded by its parent. This ClassLoader does not load any classes itself
 *     but serves as a mechanism for preventing unwanted classes from a parent from being used.
 * </p>
 * <p>
 *     Because Stateless NiFi is designed to run in an embedded environment, the classes that are available on the "System ClassLoader" (aka App ClassLoader)
 *     cannot be determined - or prevented/controlled. However, if there are conflicts between what is in the System ClassLoader and an extension (such as bringing
 *     in different versions of a popular JSON parsing library, for instance), this can cause the extensions not to function properly.
 * </p>
 * <p>
 *     Because we cannot control what is loaded by the System ClassLoader (that's up to the embedding application), the best that we can do is to block NiFi's extensions'
 *     ClassLoaders from accessing those classes. This ClassLoader allows us to do just that, allowing only specific classes that have been loaded by the parent ClassLoader
 *     to be visible/accessible by child ClassLoaders.
 * </p>
 */
public class AllowListClassLoader extends ClassLoader {
    private static final Logger logger = LoggerFactory.getLogger(AllowListClassLoader.class);

    private final Set<String> allowedClassNames;
    private final List<String> allowedModulePrefixes = Arrays.asList("java.", "jdk.");

    public AllowListClassLoader(final ClassLoader parent, final Set<String> allowed) {
        super(parent);
        this.allowedClassNames = allowed;
    }

    /**
     * @return the set of all Class names that will not be blocked from loading by the parent
     */
    public Set<String> getClassesAllowed() {
        return Collections.unmodifiableSet(allowedClassNames);
    }

    @Override
    protected Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
        if (allowedClassNames.contains(name)) {
            return super.loadClass(name, resolve);
        }

        try {
            final Class<?> found = super.loadClass(name, false);
            final boolean allowed = isClassAllowed(name, found);
            if (allowed) {
                if (resolve) {
                    super.resolveClass(found);
                }

                return found;
            }
        } catch (final NoClassDefFoundError ignored) {
            // Allow the code to 'fall through' to the ClassNotFoundException below.
        }

        throw new ClassNotFoundException(name + " was blocked by AllowListClassLoader");
    }

    @Override
    protected Class<?> findClass(final String name) throws ClassNotFoundException {
        final Class<?> found = super.findClass(name);
        if (isClassAllowed(name, found)) {
            return found;
        }

        throw new ClassNotFoundException(name + " was blocked by AllowListClassLoader");
    }

    private boolean isClassAllowed(final String name, final Class<?> clazz) {
        // If the name of the class is in the allowed class names, allow it.
        if (allowedClassNames.contains(name)) {
            return true;
        }

        // If the class has a module whose name is allowed, allow it.
        // The module is obtained by calling Class.getModule(). However, that method is only available in Java 9.
        // Since this codebase must be Java 8 compatible we can't make that method call. So we use Reflection to determine
        // if the getModule method exists (which it will for Java 9+ but not Java 1.8), and if so get the name of the module.
        try {
            final Method getModule = Class.class.getMethod("getModule");
            final Object module = getModule.invoke(clazz);
            if (module == null) {
                return false;
            }

            final Method getName = module.getClass().getMethod("getName");
            final String moduleName = (String) getName.invoke(module);
            if (isModuleAllowed(moduleName)) {
                logger.debug("Allowing Class {} because its module is {}", name, moduleName);
                return true;
            }

            return false;
        } catch (final Exception e) {
            logger.debug("Failed to determine if class {} is part of the implicitly allowed modules", name, e);
            return false;
        }
    }

    private boolean isModuleAllowed(final String moduleName) {
        for (final String prefix : allowedModulePrefixes) {
            if (moduleName.startsWith(prefix)) {
                return true;
            }
        }

        return false;
    }
}
