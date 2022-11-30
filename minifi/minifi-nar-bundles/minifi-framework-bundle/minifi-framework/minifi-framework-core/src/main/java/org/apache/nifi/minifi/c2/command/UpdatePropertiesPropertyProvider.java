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

package org.apache.nifi.minifi.c2.command;

import static org.apache.nifi.minifi.MiNiFiProperties.PROPERTIES_BY_KEY;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.nifi.c2.client.service.operation.OperandPropertiesProvider;
import org.apache.nifi.minifi.MiNiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdatePropertiesPropertyProvider implements OperandPropertiesProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdatePropertiesPropertyProvider.class);
    protected static final String AVAILABLE_PROPERTIES = "availableProperties";

    private final String bootstrapConfigFileLocation;

    public UpdatePropertiesPropertyProvider(String bootstrapConfigFileLocation) {
        this.bootstrapConfigFileLocation = bootstrapConfigFileLocation;
    }

    @Override
    public Map<String, Object> getProperties() {
        Map<String, String> bootstrapProperties = getBootstrapProperties();

        LinkedHashSet<UpdatableProperty> updatableProperties = PROPERTIES_BY_KEY.values()
            .stream()
            .filter(property -> !property.isSensitive())
            .filter(MiNiFiProperties::isModifiable)
            .map(property -> new UpdatableProperty(property.getKey(), bootstrapProperties.get(property.getKey()), property.getValidator().name()))
            .collect(Collectors.toCollection(LinkedHashSet::new));

        return Collections.singletonMap(AVAILABLE_PROPERTIES, Collections.unmodifiableSet(updatableProperties));
    }

    private Map<String, String> getBootstrapProperties() {
        Properties props = new Properties();

        File bootstrapFile = new File(bootstrapConfigFileLocation);
        try (FileInputStream fis = new FileInputStream(bootstrapFile)) {
            props.load(fis);
        } catch (FileNotFoundException e) {
            LOGGER.error("The bootstrap configuration file " + bootstrapConfigFileLocation + " doesn't exists", e);
        } catch (IOException e) {
            LOGGER.error("Failed to load properties from " + bootstrapConfigFileLocation, e);
        }
        return props.entrySet().stream()
            .collect(Collectors.toMap(entry -> (String) entry.getKey(), entry -> (String) entry.getValue()));
    }



}
