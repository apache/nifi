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
package org.apache.nifi.minifi.bootstrap;

import org.apache.nifi.minifi.bootstrap.util.BootstrapTransformer;
import org.apache.nifi.minifi.commons.schema.ProvenanceReportingSchema;
import org.apache.nifi.minifi.commons.schema.SecurityPropertiesSchema;
import org.apache.nifi.minifi.commons.schema.SensitivePropsSchema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RunMiNiFiTest {

    @Test
    public void buildSecurityPropertiesNotDefined() throws Exception {
        final Properties bootstrapProperties = getTestBootstrapProperties("bootstrap-ssl-ctx/bootstrap.conf.default");
        final Optional<SecurityPropertiesSchema> securityPropsOptional = BootstrapTransformer.buildSecurityPropertiesFromBootstrap(bootstrapProperties);
        assertFalse(securityPropsOptional.isPresent());
    }

    @Test
    public void buildSecurityPropertiesDefined() throws Exception {
        final Properties bootstrapProperties = getTestBootstrapProperties("bootstrap-ssl-ctx/bootstrap.conf.configured");
        final Optional<SecurityPropertiesSchema> securityPropsOptional = BootstrapTransformer.buildSecurityPropertiesFromBootstrap(bootstrapProperties);
        assertTrue(securityPropsOptional.isPresent());

        final SecurityPropertiesSchema securityPropertiesSchema = securityPropsOptional.get();
        assertEquals("/my/test/keystore.jks", securityPropertiesSchema.getKeystore());
        assertEquals("JKS", securityPropertiesSchema.getKeystoreType());
        assertEquals("mykeystorepassword", securityPropertiesSchema.getKeystorePassword());
        assertEquals("mykeypassword", securityPropertiesSchema.getKeyPassword());

        assertEquals("/my/test/truststore.jks", securityPropertiesSchema.getTruststore());
        assertEquals("JKS", securityPropertiesSchema.getTruststoreType());
        assertEquals("mytruststorepassword", securityPropertiesSchema.getTruststorePassword());

        assertEquals("TLS", securityPropertiesSchema.getSslProtocol());

        final SensitivePropsSchema sensitiveProps = securityPropertiesSchema.getSensitiveProps();
        assertNotNull(sensitiveProps);

        assertEquals("sensitivepropskey", sensitiveProps.getKey());
        assertEquals("algo", sensitiveProps.getAlgorithm());


        assertTrue(securityPropertiesSchema.isValid());
    }

    @Test
    public void buildSecurityPropertiesDefinedButInvalid() throws Exception {
        final Properties bootstrapProperties = getTestBootstrapProperties("bootstrap-ssl-ctx/bootstrap.conf.configured.invalid");
        final Optional<SecurityPropertiesSchema> securityPropsOptional = BootstrapTransformer.buildSecurityPropertiesFromBootstrap(bootstrapProperties);
        assertTrue(securityPropsOptional.isPresent());

        final SecurityPropertiesSchema securityPropertiesSchema = securityPropsOptional.get();
        assertEquals("/my/test/keystore.jks", securityPropertiesSchema.getKeystore());
        assertEquals("NOTAKEYSTORETYPE", securityPropertiesSchema.getKeystoreType());
        assertEquals("mykeystorepassword", securityPropertiesSchema.getKeystorePassword());
        assertEquals("mykeypassword", securityPropertiesSchema.getKeyPassword());

        assertEquals("/my/test/truststore.jks", securityPropertiesSchema.getTruststore());
        assertEquals("JKS", securityPropertiesSchema.getTruststoreType());
        assertEquals("mytruststorepassword", securityPropertiesSchema.getTruststorePassword());

        final SensitivePropsSchema sensitiveProps = securityPropertiesSchema.getSensitiveProps();
        assertNotNull(sensitiveProps);

        assertEquals("sensitivepropskey", sensitiveProps.getKey());
        assertEquals("algo", sensitiveProps.getAlgorithm());

        assertFalse(securityPropertiesSchema.isValid());
    }

    @Test
    public void buildProvenanceReportingNotDefined() throws Exception {
        final Properties bootstrapProperties = getTestBootstrapProperties("bootstrap-provenance-reporting/bootstrap.conf.default");
        final Optional<ProvenanceReportingSchema> provenanceReportingPropsOptional = BootstrapTransformer.buildProvenanceReportingPropertiesFromBootstrap(bootstrapProperties);
        assertFalse(provenanceReportingPropsOptional.isPresent());
    }

    @Test
    public void buildProvenanceReportingDefined() throws Exception {
        final Properties bootstrapProperties = getTestBootstrapProperties("bootstrap-provenance-reporting/bootstrap.conf.configured");
        final Optional<ProvenanceReportingSchema> provenanceReportingPropsOptional = BootstrapTransformer.buildProvenanceReportingPropertiesFromBootstrap(bootstrapProperties);
        assertTrue(provenanceReportingPropsOptional.isPresent());

        final ProvenanceReportingSchema provenanceReportingSchema = provenanceReportingPropsOptional.get();
        assertEquals("This is a comment!", provenanceReportingSchema.getComment());
        assertEquals("TIMER_DRIVEN", provenanceReportingSchema.getSchedulingStrategy());
        assertEquals("15 secs", provenanceReportingSchema.getSchedulingPeriod());
        assertEquals("http://localhost:8080/", provenanceReportingSchema.getDestinationUrl());
        assertEquals("provenance", provenanceReportingSchema.getPortName());
        assertEquals("http://${hostname(true)}:8081/nifi", provenanceReportingSchema.getOriginatingUrl());
        assertEquals("10 secs", provenanceReportingSchema.getTimeout());
    }


    public static Properties getTestBootstrapProperties(final String fileName) throws IOException {
        final Properties bootstrapProperties = new Properties();
        try (final InputStream fis = RunMiNiFiTest.class.getClassLoader().getResourceAsStream(fileName)) {
            bootstrapProperties.load(fis);
        }
        return bootstrapProperties;
    }

}