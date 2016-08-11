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

public class NifiPropertiesTlsClientConfigWriter implements ConfigurationWriter<TlsClientConfig> {
    public static final String HOSTNAME_PROPERTIES = "hostname.properties";
    public static final String OVERLAY_PROPERTIES = "overlay.properties";
    public static final String CONF = "./conf/";
    private final NiFiPropertiesWriterFactory niFiPropertiesWriterFactory;
    private final OutputStreamFactory outputStreamFactory;
    private final File outputFile;
    private final String hostname;
    private final int hostNum;
    private final Properties overlayProperties;
    private final Set<String> explicitProperties;

    public NifiPropertiesTlsClientConfigWriter(NiFiPropertiesWriterFactory niFiPropertiesWriterFactory, OutputStreamFactory outputStreamFactory, File outputFile, String hostname, int hostNum)
            throws IOException {
        this.niFiPropertiesWriterFactory = niFiPropertiesWriterFactory;
        this.outputStreamFactory = outputStreamFactory;
        this.outputFile = outputFile;
        this.hostname = hostname;
        this.hostNum = hostNum;
        this.overlayProperties = new Properties();
        this.overlayProperties.load(getClass().getClassLoader().getResourceAsStream(OVERLAY_PROPERTIES));
        HashSet<String> explicitProperties = new HashSet<>();
        explicitProperties.add(HOSTNAME_PROPERTIES);
        this.explicitProperties = Collections.unmodifiableSet(explicitProperties);
    }

    @Override
    public void write(TlsClientConfig tlsClientConfig) throws IOException {
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

        niFiPropertiesWriter.setPropertyValue(NiFiProperties.WEB_HTTP_HOST, "");
        niFiPropertiesWriter.setPropertyValue(NiFiProperties.WEB_HTTP_PORT, "");
        niFiPropertiesWriter.setPropertyValue(NiFiProperties.SITE_TO_SITE_SECURE, "true");

        getHostnamePropertyStream().forEach(s -> niFiPropertiesWriter.setPropertyValue(s, hostname));

        getPropertyPortMap().entrySet().forEach(nameToPortEntry -> niFiPropertiesWriter.setPropertyValue(nameToPortEntry.getKey(), Integer.toString(nameToPortEntry.getValue())));
    }

    protected Properties getOverlayProperties() {
        return overlayProperties;
    }

    protected Map<String, Integer> getPropertyPortMap() {
        return overlayProperties.stringPropertyNames().stream().filter(s -> !explicitProperties.contains(s)).collect(Collectors.toMap(Function.identity(), portProperty -> {
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

    protected Stream<String> getHostnamePropertyStream() {
        String hostnamePropertyString = overlayProperties.getProperty(HOSTNAME_PROPERTIES);
        if (!StringUtils.isEmpty(hostnamePropertyString)) {
            return Arrays.stream(hostnamePropertyString.split(",")).map(String::trim);
        }
        return Stream.of();
    }
}
