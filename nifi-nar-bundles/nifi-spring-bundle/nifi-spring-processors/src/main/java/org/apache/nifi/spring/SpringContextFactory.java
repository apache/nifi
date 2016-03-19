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
package org.apache.nifi.spring;

import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class which provides factory method to create and initialize Spring
 * Application Context while scoping it within the dedicated Class Loader.
 */
final class SpringContextFactory {

    private static final Logger logger = LoggerFactory.getLogger(SpringContextFactory.class);

    private static final String SC_DELEGATE_NAME = "org.apache.nifi.spring.bootstrap.SpringContextDelegate";

    /**
     * Creates and instance of Spring Application Context scoped within a
     * dedicated Class Loader.
     * <br>
     * The core task of this factory is to load delegate that supports message
     * exchange with Spring Application Context ({@link SpringDataExchanger})
     * using the same class loader used to load the Application Context. Such
     * class loader isolation is required to ensure that multiple instances of
     * Application Context (representing different applications) and the
     * corresponding delegate can exist per single instance of Spring NAR.
     * <br>
     * The mechanism used here is relatively simple. While
     * {@link SpringDataExchanger} is available to the current class loader and
     * would normally be loaded once per instance of NAR, the below factory
     * method first obtains class bytes for {@link SpringDataExchanger} and then
     * loads it from these bytes via ClassLoader.defineClass(..) method, thus
     * ensuring that multiple instances of {@link SpringDataExchanger} class can
     * exist and everything that is loaded within its scope is using its class
     * loader. Upon exit, the class loader is destroyed via close method
     * essentially with everything that it loaded.
     * <br>
     * Also, during the initialization of {@link SpringDataExchanger} the new
     * class loader is set as Thread.contextClassLoader ensuring that if there
     * are any libraries used by Spring beans that rely on loading resources via
     * Thread.contextClassLoader can find such resources.
     */
    static SpringDataExchanger createSpringContextDelegate(String classpath, String config) {
        List<URL> urls = gatherAdditionalClassPathUrls(classpath);
        SpringContextClassLoader contextCl = new SpringContextClassLoader(urls.toArray(new URL[] {}),
                SpringContextFactory.class.getClassLoader());
        ClassLoader tContextCl = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(contextCl);
        try {
            InputStream delegateStream = contextCl.getResourceAsStream(SC_DELEGATE_NAME.replace('.', '/') + ".class");
            byte[] delegateBytes = IOUtils.toByteArray(delegateStream);
            Class<?> clazz = contextCl.doDefineClass(SC_DELEGATE_NAME, delegateBytes, 0, delegateBytes.length);
            Constructor<?> ctr = clazz.getDeclaredConstructor(String.class);
            ctr.setAccessible(true);
            SpringDataExchanger springDelegate = (SpringDataExchanger) ctr.newInstance(config);
            if (logger.isInfoEnabled()) {
                logger.info("Successfully instantiated Spring Application Context from '" + config + "'");
            }
            return springDelegate;
        } catch (Exception e) {
            try {
                contextCl.close();
            } catch (Exception e2) {
                // ignore
            }
            throw new IllegalStateException("Failed to instantiate Spring Application Context. Config path: '" + config
                    + "'; Classpath: " + Arrays.asList(urls), e);
        } finally {
            Thread.currentThread().setContextClassLoader(tContextCl);
        }
    }

    /**
     *
     */
    static List<URL> gatherAdditionalClassPathUrls(String classPathRoot) {
        if (logger.isDebugEnabled()) {
            logger.debug("Adding additional resources from '" + classPathRoot + "' to the classpath.");
        }
        File classPathRootFile = new File(classPathRoot);
        if (classPathRootFile.exists() && classPathRootFile.isDirectory()) {
            String[] cpResourceNames = classPathRootFile.list();
            try {
                List<URL> urls = new ArrayList<>();
                for (String resourceName : cpResourceNames) {
                    File r = new File(classPathRootFile, resourceName);
                    if (r.getName().toLowerCase().endsWith(".jar") || r.isDirectory()) {
                        URL url = r.toURI().toURL();
                        urls.add(url);
                        if (logger.isDebugEnabled()) {
                            logger.debug("Identifying additional resource to the classpath: " + url);
                        }
                    }
                }
                urls.add(classPathRootFile.toURI().toURL());

                return urls;
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Failed to parse user libraries from '" + classPathRootFile.getAbsolutePath() + "'", e);
            }
        } else {
            throw new IllegalArgumentException("Path '" + classPathRootFile.getAbsolutePath()
                    + "' is not valid because it doesn't exist or does not point to a directory.");
        }
    }

    /**
     *
     */
    private static class SpringContextClassLoader extends URLClassLoader {
        /**
         *
         */
        public SpringContextClassLoader(URL[] urls, ClassLoader parent) {
            super(urls, parent);
        }

        /**
         *
         */
        public final Class<?> doDefineClass(String name, byte[] b, int off, int len) {
            return this.defineClass(name, b, off, len);
        }
    }
}
