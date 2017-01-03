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

package org.apache.nifi.controller.state.providers.local;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.components.state.StateProviderInitializationContext;
import org.apache.nifi.controller.state.StandardStateMap;
import org.apache.nifi.controller.state.StateMapSerDe;
import org.apache.nifi.controller.state.StateMapUpdate;
import org.apache.nifi.controller.state.providers.AbstractStateProvider;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wali.MinimalLockingWriteAheadLog;
import org.wali.UpdateType;
import org.wali.WriteAheadRepository;

/**
 * Provides state management for local (standalone) state, backed by a write-ahead log
 */
public class WriteAheadLocalStateProvider extends AbstractStateProvider {
    private static final Logger logger = LoggerFactory.getLogger(WriteAheadLocalStateProvider.class);

    private volatile boolean alwaysSync;

    private final StateMapSerDe serde;
    private final ConcurrentMap<String, ComponentProvider> componentProviders = new ConcurrentHashMap<>();
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory());

    static final PropertyDescriptor PATH = new PropertyDescriptor.Builder()
        .name("Directory")
        .description("The directory where the Provider should store its data")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("./state")
        .required(true)
        .build();

    static final PropertyDescriptor ALWAYS_SYNC = new PropertyDescriptor.Builder()
        .name("Always Sync")
        .description("If set to true, any change to the repository will be synchronized to the disk, meaning that NiFi will ask the operating system not to cache the information. This is very " +
                "expensive and can significantly reduce NiFi performance. However, if it is false, there could be the potential for data loss if either there is a sudden power loss or the " +
                "operating system crashes. The default value is false.")
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(true)
        .build();

    static final PropertyDescriptor NUM_PARTITIONS = new PropertyDescriptor.Builder()
        .name("Partitions")
        .description("The number of partitions.")
        .addValidator(StandardValidators.createLongValidator(1, Integer.MAX_VALUE, true))
        .defaultValue("16")
        .required(true)
        .build();

    static final PropertyDescriptor CHECKPOINT_INTERVAL = new PropertyDescriptor.Builder()
        .name("Checkpoint Interval")
        .description("The amount of time between checkpoints.")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("2 mins")
        .required(true)
        .build();


    private WriteAheadRepository<StateMapUpdate> writeAheadLog;
    private AtomicLong versionGenerator;

    public WriteAheadLocalStateProvider() {
        serde = new StateMapSerDe();
    }

    @Override
    public synchronized void init(final StateProviderInitializationContext context) throws IOException {
        long checkpointIntervalMillis = context.getProperty(CHECKPOINT_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);
        int numPartitions = context.getProperty(NUM_PARTITIONS).asInteger();
        alwaysSync = context.getProperty(ALWAYS_SYNC).asBoolean();


        final File basePath = new File(context.getProperty(PATH).getValue());

        if (!basePath.exists() && !basePath.mkdirs()) {
            throw new RuntimeException("Cannot Initialize Local State Provider because the 'Directory' property is set to \"" + basePath + "\", but that directory could not be created");
        }

        if (!basePath.isDirectory()) {
            throw new RuntimeException("Cannot Initialize Local State Provider because the 'Directory' property is set to \"" + basePath + "\", but that is a file, rather than a directory");
        }

        if (!basePath.canWrite()) {
            throw new RuntimeException("Cannot Initialize Local State Provider because the 'Directory' property is set to \"" + basePath + "\", but that directory cannot be written to");
        }

        if (!basePath.canRead()) {
            throw new RuntimeException("Cannot Initialize Local State Provider because the 'Directory' property is set to \"" + basePath + "\", but that directory cannot be read");
        }

        versionGenerator = new AtomicLong(-1L);
        writeAheadLog = new MinimalLockingWriteAheadLog<>(basePath.toPath(), numPartitions, serde, null);

        final Collection<StateMapUpdate> updates = writeAheadLog.recoverRecords();
        long maxRecordVersion = -1L;

        for (final StateMapUpdate update : updates) {
            if (update.getUpdateType() == UpdateType.DELETE) {
                continue;
            }

            final long recordVersion = update.getStateMap().getVersion();
            if (recordVersion > maxRecordVersion) {
                maxRecordVersion = recordVersion;
            }

            final String componentId = update.getComponentId();
            componentProviders.put(componentId, new ComponentProvider(writeAheadLog, versionGenerator, componentId, update.getStateMap(), alwaysSync));
        }

        // keep a separate maxRecordVersion and set it at the end so that we don't have to continually update an AtomicLong, which is more
        // expensive than just keeping track of a local 'long' variable. Since we won't actually increment this at any point until this after
        // the init() method completes, this is okay to do.
        versionGenerator.set(maxRecordVersion);

        executor.scheduleWithFixedDelay(new CheckpointTask(), checkpointIntervalMillis, checkpointIntervalMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PATH);
        properties.add(ALWAYS_SYNC);
        properties.add(CHECKPOINT_INTERVAL);
        properties.add(NUM_PARTITIONS);
        return properties;
    }

    @Override
    public synchronized void shutdown() {
        executor.shutdown();

        try {
            writeAheadLog.shutdown();
        } catch (final IOException ioe) {
            logger.warn("Failed to shut down {} successfully due to {}", this, ioe.toString());
            logger.warn("", ioe);
        }
    }

    private ComponentProvider getProvider(final String componentId) {
        ComponentProvider componentProvider = componentProviders.get(componentId);
        if (componentProvider == null) {
            final StateMap stateMap = new StandardStateMap(Collections.<String, String> emptyMap(), -1L);
            componentProvider = new ComponentProvider(writeAheadLog, versionGenerator, componentId, stateMap, alwaysSync);

            final ComponentProvider existingComponentProvider = componentProviders.putIfAbsent(componentId, componentProvider);
            if (existingComponentProvider != null) {
                componentProvider = existingComponentProvider;
            }
        }

        return componentProvider;
    }

    @Override
    public StateMap getState(final String componentId) throws IOException {
        return getProvider(componentId).getState();
    }

    @Override
    public void setState(final Map<String, String> state, final String componentId) throws IOException {
        getProvider(componentId).setState(state);
    }

    @Override
    public boolean replace(final StateMap oldValue, final Map<String, String> newValue, final String componentId) throws IOException {
        return getProvider(componentId).replace(oldValue, newValue);
    }

    @Override
    public void clear(final String componentId) throws IOException {
        getProvider(componentId).clear();
    }

    @Override
    public void onComponentRemoved(final String componentId) throws IOException {
        clear(componentId);
        componentProviders.remove(componentId);
    }

    @Override
    public Scope[] getSupportedScopes() {
        return new Scope[]{Scope.LOCAL};
    }

    private static class ComponentProvider {
        private final AtomicLong versionGenerator;
        private final WriteAheadRepository<StateMapUpdate> wal;
        private final String componentId;
        private final boolean alwaysSync;

        private StateMap stateMap;

        public ComponentProvider(final WriteAheadRepository<StateMapUpdate> wal, final AtomicLong versionGenerator, final String componentId, final StateMap stateMap, final boolean alwaysSync) {
            this.wal = wal;
            this.versionGenerator = versionGenerator;
            this.componentId = componentId;
            this.stateMap = stateMap;
            this.alwaysSync = alwaysSync;
        }

        public synchronized StateMap getState() throws IOException {
            return stateMap;
        }

        // synchronized because we need to ensure that update of state in WAL and updating of local stateMap variable is atomic.
        // Additionally, the implementation of WriteAheadRepository that we are using requires that only a single thread update the
        // repository at a time for a record with the same key. I.e., many threads can update the repository at once, as long as they
        // are not updating the repository with records that have the same identifier.
        public synchronized void setState(final Map<String, String> state) throws IOException {
            stateMap = new StandardStateMap(state, versionGenerator.incrementAndGet());
            final StateMapUpdate updateRecord = new StateMapUpdate(stateMap, componentId, UpdateType.UPDATE);
            wal.update(Collections.singleton(updateRecord), alwaysSync);
        }

        // see above explanation as to why this method is synchronized.
        public synchronized boolean replace(final StateMap oldValue, final Map<String, String> newValue) throws IOException {
            if (stateMap.getVersion() == -1L) {
                // state has never been set so return false
                return false;
            }

            if (stateMap != oldValue) {
                return false;
            }

            stateMap = new StandardStateMap(new HashMap<>(newValue), versionGenerator.incrementAndGet());
            final StateMapUpdate updateRecord = new StateMapUpdate(stateMap, componentId, UpdateType.UPDATE);
            wal.update(Collections.singleton(updateRecord), alwaysSync);
            return true;
        }

        public synchronized void clear() throws IOException {
            stateMap = new StandardStateMap(null, versionGenerator.incrementAndGet());
            final StateMapUpdate update = new StateMapUpdate(stateMap, componentId, UpdateType.UPDATE);
            wal.update(Collections.singleton(update), alwaysSync);
        }
    }

    private class CheckpointTask implements Runnable {
        @Override
        public void run() {
            try {
                logger.debug("Checkpointing Write-Ahead Log used to store components' state");

                writeAheadLog.checkpoint();
            } catch (final IOException e) {
                logger.error("Failed to checkpoint Write-Ahead Log used to store components' state", e);
            }
        }
    }

    private static class NamedThreadFactory implements ThreadFactory {
        private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

        @Override
        public Thread newThread(final Runnable r) {
            final Thread t = defaultFactory.newThread(r);
            t.setName("Write-Ahead Local State Provider Maintenance");
            t.setDaemon(true);
            return t;
        }
    }
}
