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

package org.apache.nifi.minifi.properties;

import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.ADDITIONAL_SENSITIVE_PROPERTIES_KEY;
import static org.apache.nifi.minifi.properties.ProtectedMiNiFiProperties.DEFAULT_SENSITIVE_PROPERTIES;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.nifi.minifi.commons.api.MiNiFiProperties;
import org.apache.nifi.properties.ApplicationPropertiesProtector;
import org.apache.nifi.properties.ProtectedProperties;
import org.apache.nifi.properties.SensitivePropertyProtectionException;
import org.apache.nifi.properties.SensitivePropertyProtector;
import org.apache.nifi.properties.SensitivePropertyProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtectedBootstrapProperties extends BootstrapProperties implements ProtectedProperties<BootstrapProperties>,
    SensitivePropertyProtector<ProtectedBootstrapProperties, BootstrapProperties> {

    private static final Logger logger = LoggerFactory.getLogger(ProtectedBootstrapProperties.class);

    private BootstrapProperties bootstrapProperties;

    private final SensitivePropertyProtector<ProtectedBootstrapProperties, BootstrapProperties> propertyProtectionDelegate;

    public ProtectedBootstrapProperties(BootstrapProperties props) {
        super();
        this.bootstrapProperties = props;
        this.propertyProtectionDelegate = new ApplicationPropertiesProtector<>(this);
        logger.debug("Loaded {} properties (including {} protection schemes) into ProtectedBootstrapProperties", getApplicationProperties().getPropertyKeys().size(),
            getProtectedPropertyKeys().size());
    }

    public ProtectedBootstrapProperties(Properties rawProps) {
        this(new BootstrapProperties(rawProps));
    }

    @Override
    public Set<String> getPropertyKeysIncludingProtectionSchemes() {
        return propertyProtectionDelegate.getPropertyKeysIncludingProtectionSchemes();
    }

    @Override
    public List<String> getSensitivePropertyKeys() {
        return propertyProtectionDelegate.getSensitivePropertyKeys();
    }

    @Override
    public List<String> getPopulatedSensitivePropertyKeys() {
        return propertyProtectionDelegate.getPopulatedSensitivePropertyKeys();
    }

    @Override
    public boolean hasProtectedKeys() {
        return propertyProtectionDelegate.hasProtectedKeys();
    }

    @Override
    public Map<String, String> getProtectedPropertyKeys() {
        return propertyProtectionDelegate.getProtectedPropertyKeys();
    }

    @Override
    public boolean isPropertySensitive(String key) {
        return propertyProtectionDelegate.isPropertySensitive(key);
    }

    @Override
    public boolean isPropertyProtected(String key) {
        return propertyProtectionDelegate.isPropertyProtected(key);
    }

    @Override
    public BootstrapProperties getUnprotectedProperties() throws SensitivePropertyProtectionException {
        return propertyProtectionDelegate.getUnprotectedProperties();
    }

    @Override
    public void addSensitivePropertyProvider(SensitivePropertyProvider sensitivePropertyProvider) {
        propertyProtectionDelegate.addSensitivePropertyProvider(sensitivePropertyProvider);
    }

    @Override
    public String getAdditionalSensitivePropertiesKeys() {
        return getProperty(getAdditionalSensitivePropertiesKeysName());
    }

    @Override
    public String getAdditionalSensitivePropertiesKeysName() {
        return ADDITIONAL_SENSITIVE_PROPERTIES_KEY;
    }

    @Override
    public List<String> getDefaultSensitiveProperties() {
        return Stream.of(DEFAULT_SENSITIVE_PROPERTIES, Arrays.stream(MiNiFiProperties.values()).filter(MiNiFiProperties::isSensitive).map(MiNiFiProperties::getKey).collect(Collectors.toList()))
            .flatMap(List::stream).distinct().collect(Collectors.toList());
    }

    @Override
    public BootstrapProperties getApplicationProperties() {
        if (this.bootstrapProperties == null) {
            this.bootstrapProperties = new BootstrapProperties();
        }

        return this.bootstrapProperties;
    }

    @Override
    public BootstrapProperties createApplicationProperties(Properties rawProperties) {
        return new BootstrapProperties(rawProperties);
    }
}
