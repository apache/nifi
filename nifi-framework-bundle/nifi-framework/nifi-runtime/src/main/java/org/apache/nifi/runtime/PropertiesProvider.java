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
package org.apache.nifi.runtime;

import org.apache.nifi.util.NiFiBootstrapPropertiesLoader;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * Properties Provider abstracts loading Application Properties based on available Class Loader
 */
class PropertiesProvider {

    private static final Logger logger = LoggerFactory.getLogger(PropertiesProvider.class);

    private static final String LOADER_CLASS = "org.apache.nifi.properties.NiFiPropertiesLoader";

    private static final String GET_METHOD = "get";

    /**
     * Read Properties without using Properties Loader
     *
     * @return Application Properties
     */
    static NiFiProperties readProperties() {
        final NiFiBootstrapPropertiesLoader bootstrapPropertiesLoader = new NiFiBootstrapPropertiesLoader();
        final String propertiesFilePath = bootstrapPropertiesLoader.getDefaultApplicationPropertiesFilePath();
        logger.info("Loading Application Properties [{}]", propertiesFilePath);
        return NiFiProperties.createBasicNiFiProperties(propertiesFilePath);
    }

    /**
     * Read Properties using Properties Loader provided in Class Loader
     *
     * @param propertiesClassLoader Properties Class Loader from Framework NAR
     * @return Application Properties
     */
    static NiFiProperties readProperties(final ClassLoader propertiesClassLoader) {
        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(propertiesClassLoader);

        try {
            final Class<?> loaderClass = Class.forName(LOADER_CLASS, true, propertiesClassLoader);
            final Object loader = loaderClass.getConstructor().newInstance();
            final Method getMethod = loaderClass.getMethod(GET_METHOD);
            return (NiFiProperties) getMethod.invoke(loader);
        } catch (final Exception e) {
            throw new IllegalStateException("Application Properties loading failed", e);
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }
}
