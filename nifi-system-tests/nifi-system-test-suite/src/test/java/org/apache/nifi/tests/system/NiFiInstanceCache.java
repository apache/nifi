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

package org.apache.nifi.tests.system;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public class NiFiInstanceCache {
    private static final Logger logger = LoggerFactory.getLogger(NiFiInstanceCache.class);

    private final CachedInstance standaloneInstance = new CachedInstance();
    private final CachedInstance clusteredInstance = new CachedInstance();

    public NiFiInstance createInstance(final NiFiInstanceFactory instanceFactory, final String testName, final boolean allowReuse) throws IOException {
        final CachedInstance cachedInstance = instanceFactory.isClusteredInstance() ? clusteredInstance : standaloneInstance;

        if (!allowReuse) {
            logger.info("Will create new NiFi instance for {} because the test does not allow reuse of the created instance", testName);
            cachedInstance.shutdown();
            cachedInstance.setFactory(null, testName);

            return instanceFactory.createInstance();
        }

        if (cachedInstance.getFactory() == null) {
            cachedInstance.setFactory(instanceFactory, testName);
            return cachedInstance.getOrCreateInstance();
        }

        if (Objects.equals(cachedInstance.getFactory(), instanceFactory)) {
            logger.info("Will use cached Nifi instance that was created for test {} in order to run test {} if there is one", cachedInstance.getTestName(), testName);
            return cachedInstance.getOrCreateInstance();
        }

        logger.info("Cached NiFi Instance created for test {} differs in configuration from what is necessary for test {}. Will shutdown cached instance.", cachedInstance.getTestName(), testName);
        cachedInstance.shutdown();
        cachedInstance.setFactory(instanceFactory, testName);
        return cachedInstance.getOrCreateInstance();
    }

    public void shutdown() {
        standaloneInstance.shutdown();
        clusteredInstance.shutdown();
    }

    public void stopOrRecycle(final NiFiInstance nifiInstance) {
        if (nifiInstance instanceof CachedNiFiInstance) {
            ((CachedNiFiInstance) nifiInstance).ensureProperState();
            return;
        }

        nifiInstance.stop();
    }

    public void poison(final NiFiInstance nifiInstance) {
        if (nifiInstance == null) {
            return;
        }

        nifiInstance.stop();

        standaloneInstance.poison(nifiInstance);
        clusteredInstance.poison(nifiInstance);
    }

    private static class CachedInstance {
        private NiFiInstanceFactory factory;
        private CachedNiFiInstance instance;
        private String testName;

        public void setFactory(final NiFiInstanceFactory factory, final String testName) {
            this.factory = factory;
            this.testName = testName;
        }

        public String getTestName() {
            return testName;
        }

        public NiFiInstanceFactory getFactory() {
            return factory;
        }

        public void poison(final NiFiInstance toPoison) {
            if (this.instance == null) {
                return;
            }
            final NiFiInstance rawInstance = this.instance.getRawInstance();

            if (Objects.equals(rawInstance, toPoison)) {
                logger.info("{} has been poisoned. Will not reuse this NiFi instance", rawInstance);
                this.instance = null;
            }
        }

        public NiFiInstance getOrCreateInstance() throws IOException {
            if (instance != null) {
                return instance;
            }

            final NiFiInstance rawInstance = factory.createInstance();
            this.instance = new CachedNiFiInstance(rawInstance);
            return this.instance;
        }

        public void shutdown() {
            if (instance != null) {
                instance.stop();
            }

            instance = null;
        }
    }


    private static class CachedNiFiInstance implements NiFiInstance {
        private final NiFiInstance rawInstance;
        private boolean envCreated = false;
        private boolean started = false;
        private boolean requireRestart;

        public CachedNiFiInstance(final NiFiInstance rawInstance) {
            this.rawInstance = rawInstance;
        }

        public NiFiInstance getRawInstance() {
            return this.rawInstance;
        }

        public void ensureProperState() {
            if (rawInstance.getNumberOfNodes() == 1) {
                return;
            }

            if (!rawInstance.isAccessible()) {
                logger.info("NiFi Instance {} is not accessible so will stop the instance to ensure proper state for the next test", rawInstance);
                stop();
            }
        }

        @Override
        public boolean isAccessible() {
            return rawInstance.isAccessible();
        }

        @Override
        public void createEnvironment() throws IOException {
            if (envCreated) {
                return;
            }

            rawInstance.createEnvironment();
            envCreated = true;
        }

        @Override
        public void start(final boolean waitForCompletion) {
            if (started && requireRestart) {
                logger.info("Must restart NiFi Instance {} before use", rawInstance);

                rawInstance.stop();
                started = false;
                requireRestart = false;
            }

            if (started) {
                logger.info("NiFi Instance {} is already started", rawInstance);
                return;
            }

            rawInstance.start(waitForCompletion);
            started = true;
        }

        @Override
        public void stop() {
            logger.info("Stopping NiFi Instance {}", rawInstance);
            started = false;
            rawInstance.stop();
        }

        @Override
        public boolean isClustered() {
            return rawInstance.isClustered();
        }

        @Override
        public int getNumberOfNodes() {
            return rawInstance.getNumberOfNodes();
        }

        @Override
        public int getNumberOfNodes(final boolean includeOnlyAutoStartInstances) {
            return rawInstance.getNumberOfNodes(includeOnlyAutoStartInstances);
        }

        @Override
        public NiFiInstance getNodeInstance(final int nodeIndex) {
            return rawInstance.getNodeInstance(nodeIndex);
        }

        @Override
        public Properties getProperties() throws IOException {
            return rawInstance.getProperties();
        }

        @Override
        public File getInstanceDirectory() {
            return rawInstance.getInstanceDirectory();
        }

        @Override
        public boolean isAutoStart() {
            return rawInstance.isAutoStart();
        }

        @Override
        public void setProperty(final String propertyName, final String propertyValue) throws IOException {
            rawInstance.setProperty(propertyName, propertyValue);
            requireRestart = true;

            logger.info("Setting property {} on NiFi Instance {}. This will require that the instance be restarted.", propertyName, rawInstance);
        }

        @Override
        public void setProperties(final Map<String, String> properties) throws IOException {
            rawInstance.setProperties(properties);
            requireRestart = true;

            logger.info("Setting multiple properties on NiFi Instance {}. This will require that the instance be restarted.", rawInstance);
        }

        @Override
        public void quarantineTroubleshootingInfo(final File directory, final Throwable failureCause) throws IOException {
            rawInstance.quarantineTroubleshootingInfo(directory, failureCause);
        }
    }
}
