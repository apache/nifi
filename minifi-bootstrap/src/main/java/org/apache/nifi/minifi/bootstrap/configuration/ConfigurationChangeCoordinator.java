/**
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

import org.apache.nifi.minifi.bootstrap.ConfigurationFileHolder;
import org.apache.nifi.minifi.bootstrap.configuration.ingestors.interfaces.ChangeIngestor;
import org.apache.nifi.minifi.bootstrap.util.ByteBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class ConfigurationChangeCoordinator implements Closeable, ConfigurationChangeNotifier {

    public static final String NOTIFIER_PROPERTY_PREFIX = "nifi.minifi.notifier";
    public static final String NOTIFIER_INGESTORS_KEY = NOTIFIER_PROPERTY_PREFIX + ".ingestors";
    private final static Logger logger = LoggerFactory.getLogger(ConfigurationChangeCoordinator.class);
    private final Set<ConfigurationChangeListener> configurationChangeListeners = new HashSet<>();
    private final Set<ChangeIngestor> changeIngestors = new HashSet<>();

    /**
     * Provides an opportunity for the implementation to perform configuration and initialization based on properties received from the bootstrapping configuration
     *
     * @param properties from the bootstrap configuration
     */
    public void initialize(Properties properties, ConfigurationFileHolder configurationFileHolder, Collection<ConfigurationChangeListener> changeListenerSet) {
        final String ingestorsCsv = properties.getProperty(NOTIFIER_INGESTORS_KEY);

        if (ingestorsCsv != null && !ingestorsCsv.isEmpty()) {
            for (String ingestorClassname : Arrays.asList(ingestorsCsv.split(","))) {
                ingestorClassname = ingestorClassname.trim();
                try {
                    Class<?> ingestorClass = Class.forName(ingestorClassname);
                    ChangeIngestor changeIngestor = (ChangeIngestor) ingestorClass.newInstance();
                    changeIngestor.initialize(properties, configurationFileHolder, this);
                    changeIngestors.add(changeIngestor);
                    logger.info("Initialized ");
                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                    throw new RuntimeException("Issue instantiating ingestor " + ingestorClassname, e);
                }
            }
        }
        configurationChangeListeners.clear();
        configurationChangeListeners.addAll(changeListenerSet);
    }

    /**
     * Begins the associated notification service provided by the given implementation.  In most implementations, no action will occur until this method is invoked.
     */
    public void start() {
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
        logger.info("Notifying Listeners of a change");

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
            logger.info("Listener notification result:" + result.toString());
        }
        return listenerHandleResults;
    }


    @Override
    public void close() throws IOException {
        for (ChangeIngestor changeIngestor : changeIngestors) {
            changeIngestor.close();
        }
    }
}
