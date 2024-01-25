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

import static java.util.Optional.ofNullable;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toList;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.nifi.minifi.bootstrap.RunMiNiFi;
import org.apache.nifi.minifi.bootstrap.configuration.ingestors.interfaces.ChangeIngestor;
import org.apache.nifi.minifi.bootstrap.service.BootstrapFileProvider;
import org.apache.nifi.minifi.bootstrap.util.ByteBufferInputStream;
import org.apache.nifi.minifi.properties.BootstrapProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurationChangeCoordinator implements Closeable, ConfigurationChangeNotifier {

    public static final String NOTIFIER_INGESTORS_KEY = "nifi.minifi.notifier.ingestors";

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationChangeCoordinator.class);
    private static final String COMMA = ",";

    private final BootstrapFileProvider bootstrapFileProvider;
    private final RunMiNiFi runMiNiFi;
    private final Set<ConfigurationChangeListener> configurationChangeListeners;
    private final Set<ChangeIngestor> changeIngestors;

    public ConfigurationChangeCoordinator(BootstrapFileProvider bootstrapFileProvider, RunMiNiFi runMiNiFi,
                                          Set<ConfigurationChangeListener> miNiFiConfigurationChangeListeners) {
        this.bootstrapFileProvider = bootstrapFileProvider;
        this.runMiNiFi = runMiNiFi;
        this.configurationChangeListeners = ofNullable(miNiFiConfigurationChangeListeners).map(Collections::unmodifiableSet).orElse(Collections.emptySet());
        this.changeIngestors = new HashSet<>();
    }

    @Override
    public Collection<ListenerHandleResult> notifyListeners(ByteBuffer newFlowConfig) {
        LOGGER.info("Notifying Listeners of a change");
        return configurationChangeListeners.stream()
            .map(listener -> notifyListener(newFlowConfig, listener))
            .collect(toList());
    }

    @Override
    public void close() {
        closeIngestors();
    }

    /**
     * Begins the associated notification service provided by the given implementation.  In most implementations, no action will occur until this method is invoked.
     */
    public void start() throws IOException {
        initialize();
        changeIngestors.forEach(ChangeIngestor::start);
    }

    private ListenerHandleResult notifyListener(ByteBuffer newFlowConfig, ConfigurationChangeListener listener) {
        try {
            listener.handleChange(new ByteBufferInputStream(newFlowConfig.duplicate()));
            ListenerHandleResult listenerHandleResult = new ListenerHandleResult(listener);
            LOGGER.info("Listener notification result {}", listenerHandleResult);
            return listenerHandleResult;
        } catch (ConfigurationChangeException ex) {
            ListenerHandleResult listenerHandleResult = new ListenerHandleResult(listener, ex);
            LOGGER.error("Listener notification result {} with failure {}", listenerHandleResult, ex);
            return listenerHandleResult;
        }
    }

    private void initialize() throws IOException {
        closeIngestors();

        BootstrapProperties bootstrapProperties = bootstrapFileProvider.getBootstrapProperties();
        ofNullable(bootstrapProperties.getProperty(NOTIFIER_INGESTORS_KEY))
            .filter(not(String::isBlank))
            .map(ingestors -> ingestors.split(COMMA))
            .stream()
            .flatMap(Stream::of)
            .map(String::trim)
            .forEach(ingestorClassname -> instantiateIngestor(bootstrapProperties, ingestorClassname));
    }

    private void closeIngestors() {
        try {
            for (ChangeIngestor changeIngestor : changeIngestors) {
                changeIngestor.close();
            }
            changeIngestors.clear();
        } catch (IOException e) {
            LOGGER.warn("Could not successfully stop notifiers", e);
        }
    }

    private void instantiateIngestor(BootstrapProperties bootstrapProperties, String ingestorClassname) {
        try {
            Class<?> ingestorClass = Class.forName(ingestorClassname);
            ChangeIngestor changeIngestor = (ChangeIngestor) ingestorClass.getDeclaredConstructor().newInstance();
            changeIngestor.initialize(bootstrapProperties, runMiNiFi, this);
            changeIngestors.add(changeIngestor);
            LOGGER.info("Initialized ingestor: {}", ingestorClassname);
        } catch (final Exception e) {
            LOGGER.error("Instantiating [{}] ingestor failed", ingestorClassname, e);
        }
    }
}
