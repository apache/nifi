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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.registry.VariableRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A file based variable registry that loads all properties from files specified
 * during construction and is backed by system properties and environment
 * variables accessible to the JVM.
 */
public class FileBasedVariableRegistry implements VariableRegistry {

    private final static Logger LOG = LoggerFactory.getLogger(FileBasedVariableRegistry.class);
    final Map<VariableDescriptor, String> map;

    public FileBasedVariableRegistry(final Path[] propertiesPaths) {
        final Map<VariableDescriptor, String> newMap = new HashMap<>(VariableRegistry.ENVIRONMENT_SYSTEM_REGISTRY.getVariableMap());
        final int systemEnvPropCount = newMap.size();
        int totalPropertiesLoaded = systemEnvPropCount;
        LOG.info("Loaded {} properties from system properties and environment variables",systemEnvPropCount);
        try {
            for (final Path path : propertiesPaths) {
                if (Files.exists(path)) {
                    final AtomicInteger propsLoaded = new AtomicInteger(0);
                    try (final InputStream inStream = new BufferedInputStream(new FileInputStream(path.toFile()))) {
                        Properties properties = new Properties();
                        properties.load(inStream);
                        properties.entrySet().stream().forEach((entry) -> {
                            final VariableDescriptor desc = new VariableDescriptor.Builder(entry.getKey().toString())
                                    .description(path.toString())
                                    .sensitive(false)
                                    .build();
                            newMap.put(desc, entry.getValue().toString());
                            propsLoaded.incrementAndGet();
                        });
                    }
                    totalPropertiesLoaded += propsLoaded.get();
                    if(propsLoaded.get() > 0){
                        LOG.info("Loaded {} properties from '{}'", propsLoaded.get(), path);
                    }else{
                        LOG.warn("No properties loaded from '{}'", path);
                    }
                } else {
                    LOG.warn("Skipping property file {} as it does not appear to exist", path);
                }
            }
        } catch (final IOException ioe) {
            LOG.error("Unable to complete variable registry loading from files due to ", ioe);
        }
        LOG.info("Loaded a total of {} properties.  Including precedence overrides effective accessible registry key size is {}", totalPropertiesLoaded, newMap.size());
        map = newMap;
    }

    @Override
    public Map<VariableDescriptor, String> getVariableMap() {
        return Collections.unmodifiableMap(map);
    }

}
