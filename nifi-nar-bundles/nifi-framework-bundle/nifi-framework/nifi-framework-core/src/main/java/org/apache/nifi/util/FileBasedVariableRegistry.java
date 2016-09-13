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
package org.apache.nifi.util;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.monitor.MD5SumMonitor;
import org.apache.nifi.util.file.monitor.SynchronousFileWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A file based variable registry that loads all properties from files specified
 * during construction and is backed by system properties and environment
 * variables accessible to the JVM.
 */
public class FileBasedVariableRegistry implements VariableRegistry {

    private final static Logger LOG = LoggerFactory.getLogger(FileBasedVariableRegistry.class);
    final ConcurrentMap<VariableDescriptor, String> variables;
    final ConcurrentMap<Path, SynchronousFileWatcher> watchers;
    final long checkMillis;

    public FileBasedVariableRegistry(final Path[] propertiesPaths, final long checkMillis) {
        this.checkMillis = checkMillis;

        final ConcurrentMap<VariableDescriptor, String> variables = new ConcurrentHashMap<>(VariableRegistry.ENVIRONMENT_SYSTEM_REGISTRY.getVariableMap());
        final ConcurrentMap<Path, SynchronousFileWatcher> watchers = new ConcurrentHashMap<>();
        final int systemEnvPropCount = variables.size();
        int totalPropertiesLoaded = systemEnvPropCount;

        LOG.info("Loaded {} properties from system properties and environment variables", systemEnvPropCount);

        try {
            for (final Path path : propertiesPaths) {
                totalPropertiesLoaded += loadProperties(path, variables, watchers, checkMillis);
            }
        } catch (final IOException ioe) {
            LOG.error("Unable to complete variable registry loading from files due to ", ioe);
        }

        LOG.info("Loaded a total of {} properties.  Including precedence overrides effective accessible registry key size is {}", totalPropertiesLoaded, variables.size());

        this.variables = variables;
        this.watchers = watchers;
    }

    public FileBasedVariableRegistry(final Path[] propertiesPaths, final String checkSchedule) {
        this(propertiesPaths, StringUtils.isNotBlank(checkSchedule) ? FormatUtils.getTimeDuration(checkSchedule, TimeUnit.MILLISECONDS) : 0L);
    }

    public FileBasedVariableRegistry(final Path[] propertiesPaths) {
        this(propertiesPaths, NiFiProperties.createBasicNiFiProperties(null, null).getVariableRegistryCheckSchedule());
    }

    private int loadProperties(final Path path, final Map<VariableDescriptor, String> variables, final Map<Path, SynchronousFileWatcher> watchers, final long checkMillis) throws IOException {
        int propertiesLoaded = 0;
        if (Files.exists(path)) {
            if (watchers != null && !watchers.containsKey(path)) {
                if (checkMillis > 0) {
                    watchers.put(path, new SynchronousFileWatcher(path, new MD5SumMonitor(), checkMillis));
                } else {
                    LOG.warn("Check schedule is zero, properties from {} will not be reloadable", path);
                }
            }

            final AtomicInteger propsLoaded = new AtomicInteger(0);
            try (final InputStream inStream = new BufferedInputStream(new FileInputStream(path.toFile()))) {
                Properties properties = new Properties();
                properties.load(inStream);
                properties.entrySet().stream().forEach((entry) -> {
                    final VariableDescriptor desc = new VariableDescriptor.Builder(entry.getKey().toString())
                            .description(path.toString())
                            .sensitive(false)
                            .build();
                    variables.put(desc, entry.getValue().toString());
                    propsLoaded.incrementAndGet();
                });
            }
            propertiesLoaded += propsLoaded.get();

            if (propsLoaded.get() > 0) {
                LOG.info("Loaded {} properties from '{}'", propsLoaded.get(), path);
            } else {
                LOG.warn("No properties loaded from '{}'", path);
            }
        } else {
            LOG.warn("Skipping property file {} as it does not appear to exist", path);
        }
        return propertiesLoaded;
    }

    @Override
    public Map<VariableDescriptor, String> getVariableMap() {
        if (watchers != null && !watchers.isEmpty()) {
            int propsReloaded = 0;

            for (Map.Entry<Path, SynchronousFileWatcher> e : watchers.entrySet()) {
                final Path path = e.getKey();
                final SynchronousFileWatcher watcher = e.getValue();
                try {
                    if (watcher.checkAndReset()) {
                        propsReloaded += loadProperties(path, variables, watchers, checkMillis);
                    }
                } catch (final IOException ioe) {
                    LOG.error("Unable to reload variables from {} due to {}", new Object[]{path, ioe.getMessage()}, ioe);
                }
            }

            if (propsReloaded > 0) {
                LOG.info("Reloaded {} properties", propsReloaded);
            } else {
                LOG.debug("No properties reloaded");
            }
        }

        return Collections.unmodifiableMap(variables);
    }

}
