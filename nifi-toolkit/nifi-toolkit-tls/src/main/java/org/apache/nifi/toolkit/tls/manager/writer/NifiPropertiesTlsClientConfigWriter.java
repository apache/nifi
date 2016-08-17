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

package org.apache.nifi.toolkit.tls.manager.writer;

import org.apache.nifi.toolkit.tls.configuration.TlsClientConfig;
import org.apache.nifi.toolkit.tls.properties.NiFiPropertiesWriter;
import org.apache.nifi.toolkit.tls.properties.NiFiPropertiesWriterFactory;
import org.apache.nifi.toolkit.tls.util.OutputStreamFactory;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Will write a nifi.properties file appropriate for the given client config
 */
public class NifiPropertiesTlsClientConfigWriter implements ConfigurationWriter<TlsClientConfig> {
    public static final String HOSTNAME_PROPERTIES = "hostname.properties";
    public static final String OVERLAY_PROPERTIES = "overlay.properties";
    public static final String INCREMENTING_PROPERTIES = "incrementing.properties";
    public static final String CONF = "./conf/";
    private final NiFiPropertiesWriterFactory niFiPropertiesWriterFactory;
    private final File outputFile;
    private final String hostname;
    private final int hostNum;
    private final Properties overlayProperties;
    private final Set<String> metaProperties;

    public NifiPropertiesTlsClientConfigWriter(NiFiPropertiesWriterFactory niFiPropertiesWriterFactory, File outputFile, String hostname, int hostNum) throws IOException {
        this.niFiPropertiesWriterFactory = niFiPropertiesWriterFactory;
        this.outputFile = outputFile;
        this.hostname = hostname;
        this.hostNum = hostNum;
        this.overlayProperties = new Properties();
        this.overlayProperties.load(getClass().getClassLoader().getResourceAsStream(OVERLAY_PROPERTIES));
        HashSet<String> metaProperties = new HashSet<>();
        metaProperties.add(HOSTNAME_PROPERTIES);
        metaProperties.add(INCREMENTING_PROPERTIES);
        getIncrementingPropertiesStream().forEach(metaProperties::add);
        this.metaProperties = Collections.unmodifiableSet(metaProperties);
    }

    @Override
    public void write(TlsClientConfig tlsClientConfig, OutputStreamFactory outputStreamFactory) throws IOException {
        NiFiPropertiesWriter niFiPropertiesWriter = niFiPropertiesWriterFactory.create();
        updateProperties(niFiPropertiesWriter, tlsClientConfig);
        try (OutputStream stream = outputStreamFactory.create(outputFile)) {
            niFiPropertiesWriter.writeNiFiProperties(stream);
        }
    }

    protected void updateProperties(NiFiPropertiesWriter niFiPropertiesWriter, TlsClientConfig tlsClientConfig) throws IOException {
        niFiPropertiesWriter.setPropertyValue(NiFiProperties.SECURITY_KEYSTORE, CONF + new File(tlsClientConfig.getKeyStore()).getName());
        niFiPropertiesWriter.setPropertyValue(NiFiProperties.SECURITY_KEYSTORE_TYPE, tlsClientConfig.getKeyStoreType());
        niFiPropertiesWriter.setPropertyValue(NiFiProperties.SECURITY_KEYSTORE_PASSWD, tlsClientConfig.getKeyStorePassword());
        niFiPropertiesWriter.setPropertyValue(NiFiProperties.SECURITY_KEY_PASSWD, tlsClientConfig.getKeyPassword());

        niFiPropertiesWriter.setPropertyValue(NiFiProperties.SECURITY_TRUSTSTORE, CONF + new File(tlsClientConfig.getTrustStore()).getName());
        niFiPropertiesWriter.setPropertyValue(NiFiProperties.SECURITY_TRUSTSTORE_TYPE, tlsClientConfig.getTrustStoreType());
        niFiPropertiesWriter.setPropertyValue(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD, tlsClientConfig.getTrustStorePassword());

        getHostnamePropertyStream().forEach(s -> niFiPropertiesWriter.setPropertyValue(s, hostname));

        overlayProperties.stringPropertyNames().stream().filter(s -> !metaProperties.contains(s)).forEach(s -> niFiPropertiesWriter.setPropertyValue(s, overlayProperties.getProperty(s)));

        getIncrementingPropertyMap().entrySet().forEach(nameToIntegerEntry -> niFiPropertiesWriter.setPropertyValue(nameToIntegerEntry.getKey(), Integer.toString(nameToIntegerEntry.getValue())));
    }

    protected Properties getOverlayProperties() {
        return overlayProperties;
    }

    protected Map<String, Integer> getIncrementingPropertyMap() {
        return getIncrementingPropertiesStream().collect(Collectors.toMap(Function.identity(), portProperty -> {
            String portVal = overlayProperties.getProperty(portProperty);
            int startingPort;
            try {
                startingPort = Integer.parseInt(portVal);
            } catch (NumberFormatException e) {
                throw new NumberFormatException("Expected numeric values in " + OVERLAY_PROPERTIES + " (" + portProperty + " was " + portVal + ")");
            }
            return startingPort + hostNum - 1;
        }));
    }

    protected Stream<String> getIncrementingPropertiesStream() {
        return getCommaSeparatedPropertyStream(INCREMENTING_PROPERTIES);
    }

    protected Stream<String> getHostnamePropertyStream() {
        return getCommaSeparatedPropertyStream(HOSTNAME_PROPERTIES);
    }

    private Stream<String> getCommaSeparatedPropertyStream(String property) {
        String hostnamePropertyString = overlayProperties.getProperty(property);
        if (!StringUtils.isEmpty(hostnamePropertyString)) {
            return Arrays.stream(hostnamePropertyString.split(",")).map(String::trim);
        }
        return Stream.of();
    }
}
