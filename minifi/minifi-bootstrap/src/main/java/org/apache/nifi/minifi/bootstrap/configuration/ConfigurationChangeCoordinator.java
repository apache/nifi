/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.minifi.bootstrap.configuration;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import org.apache.nifi.minifi.bootstrap.RunMiNiFi;
import org.apache.nifi.minifi.bootstrap.configuration.ingestors.interfaces.ChangeIngestor;
import org.apache.nifi.minifi.bootstrap.util.ByteBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurationChangeCoordinator implements Closeable, ConfigurationChangeNotifier {

    public static final String NOTIFIER_PROPERTY_PREFIX = "nifi.minifi.notifier";
    public static final String NOTIFIER_INGESTORS_KEY = NOTIFIER_PROPERTY_PREFIX + ".ingestors";
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationChangeCoordinator.class);

    private final Set<ConfigurationChangeListener> configurationChangeListeners;
    private final Set<ChangeIngestor> changeIngestors = new HashSet<>();

    private final Properties bootstrapProperties;
    private final RunMiNiFi runMiNiFi;

    public ConfigurationChangeCoordinator(Properties bootstrapProperties, RunMiNiFi runMiNiFi,
        Set<ConfigurationChangeListener> miNiFiConfigurationChangeListeners) {
        this.bootstrapProperties = bootstrapProperties;
        this.runMiNiFi = runMiNiFi;
        this.configurationChangeListeners = Optional.ofNullable(miNiFiConfigurationChangeListeners).map(Collections::unmodifiableSet).orElse(Collections.emptySet());
    }

    /**
     * Begins the associated notification service provided by the given implementation.  In most implementations, no action will occur until this method is invoked.
     */
    public void start() {
        initialize();
        changeIngestors.forEach(ChangeIngestor::start);
    }

    /**
     * Provides an immutable collection of listeners for the notifier instance
     *
     * @return a collection of those listeners registered for notifications
     */
    public Set<ConfigurationChangeListener> getChangeListeners() {
        return Collections.unmodifiableSet(configurationChangeListeners);
    }

    /**
     * Provide the mechanism by which listeners are notified
     */
    public Collection<ListenerHandleResult> notifyListeners(ByteBuffer newConfig) {
        LOGGER.info("Notifying Listeners of a change");

        Collection<ListenerHandleResult> listenerHandleResults = new ArrayList<>(configurationChangeListeners.size());
        for (final ConfigurationChangeListener listener : getChangeListeners()) {
            ListenerHandleResult result;
            try {
                listener.handleChange(new ByteBufferInputStream(newConfig.duplicate()));
                result = new ListenerHandleResult(listener);
            } catch (ConfigurationChangeException ex) {
                result = new ListenerHandleResult(listener, ex);
            }
            listenerHandleResults.add(result);
            LOGGER.info("Listener notification result: {}", result);
        }
        return listenerHandleResults;
    }


    @Override
    public void close() {
        try {
            for (ChangeIngestor changeIngestor : changeIngestors) {
                changeIngestor.close();
            }
            changeIngestors.clear();
        } catch (IOException e) {
            LOGGER.warn("Could not successfully stop notifiers", e);
        }
    }

    private void initialize() {
        close();
        // cleanup previously initialized ingestors
        String ingestorsCsv = bootstrapProperties.getProperty(NOTIFIER_INGESTORS_KEY);

        if (ingestorsCsv != null && !ingestorsCsv.isEmpty()) {
            for (String ingestorClassname : ingestorsCsv.split(",")) {
                ingestorClassname = ingestorClassname.trim();
                try {
                    Class<?> ingestorClass = Class.forName(ingestorClassname);
                    ChangeIngestor changeIngestor = (ChangeIngestor) ingestorClass.newInstance();
                    changeIngestor.initialize(bootstrapProperties, runMiNiFi, this);
                    changeIngestors.add(changeIngestor);
                    LOGGER.info("Initialized ingestor: {}", ingestorClassname);
                } catch (Exception e) {
                    LOGGER.error("Instantiating [{}] ingestor failed", ingestorClassname, e);
                }
            }
        }
    }
}
