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
import java.nio.file.Path;
import java.nio.file.Paths;

public class NifiPropertiesTlsClientConfigWriter implements ConfigurationWriter<TlsClientConfig> {
    private final NiFiPropertiesWriterFactory niFiPropertiesWriterFactory;
    private final OutputStreamFactory outputStreamFactory;
    private final File file;
    private final String hostname;
    private final String httpsPort;

    public NifiPropertiesTlsClientConfigWriter(NiFiPropertiesWriterFactory niFiPropertiesWriterFactory, OutputStreamFactory outputStreamFactory, File file, String hostname, String httpsPort) {
        this.niFiPropertiesWriterFactory = niFiPropertiesWriterFactory;
        this.outputStreamFactory = outputStreamFactory;
        this.file = file;
        this.hostname = hostname;
        this.httpsPort = httpsPort;
    }

    @Override
    public void write(TlsClientConfig tlsClientConfig) throws IOException {
        NiFiPropertiesWriter niFiPropertiesWriter = niFiPropertiesWriterFactory.create();
        updateProperties(niFiPropertiesWriter, tlsClientConfig);
        try (OutputStream stream = outputStreamFactory.create(file)) {
            niFiPropertiesWriter.writeNiFiProperties(stream);
        }
    }

    protected void updateProperties(NiFiPropertiesWriter niFiPropertiesWriter, TlsClientConfig tlsClientConfig) {
        Path parentPath = Paths.get(file.getParentFile().getAbsolutePath());
        niFiPropertiesWriter.setPropertyValue(NiFiProperties.SECURITY_KEYSTORE, parentPath.relativize(Paths.get(tlsClientConfig.getKeyStore())).toString());
        niFiPropertiesWriter.setPropertyValue(NiFiProperties.SECURITY_KEYSTORE_TYPE, tlsClientConfig.getKeyStoreType());
        niFiPropertiesWriter.setPropertyValue(NiFiProperties.SECURITY_KEYSTORE_PASSWD, tlsClientConfig.getKeyStorePassword());
        niFiPropertiesWriter.setPropertyValue(NiFiProperties.SECURITY_KEY_PASSWD, tlsClientConfig.getKeyPassword());
        niFiPropertiesWriter.setPropertyValue(NiFiProperties.SECURITY_TRUSTSTORE, parentPath.relativize(Paths.get(tlsClientConfig.getTrustStore())).toString());
        niFiPropertiesWriter.setPropertyValue(NiFiProperties.SECURITY_TRUSTSTORE_TYPE, tlsClientConfig.getTrustStoreType());
        niFiPropertiesWriter.setPropertyValue(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD, tlsClientConfig.getTrustStorePassword());
        if (!StringUtils.isEmpty(httpsPort)) {
            if (!StringUtils.isEmpty(hostname)) {
                niFiPropertiesWriter.setPropertyValue(NiFiProperties.WEB_HTTPS_HOST, hostname);
            }
            niFiPropertiesWriter.setPropertyValue(NiFiProperties.WEB_HTTPS_PORT, httpsPort);
            niFiPropertiesWriter.setPropertyValue(NiFiProperties.WEB_HTTP_HOST, "");
            niFiPropertiesWriter.setPropertyValue(NiFiProperties.WEB_HTTP_PORT, "");
            niFiPropertiesWriter.setPropertyValue(NiFiProperties.SITE_TO_SITE_SECURE, "true");
        }
    }
}
