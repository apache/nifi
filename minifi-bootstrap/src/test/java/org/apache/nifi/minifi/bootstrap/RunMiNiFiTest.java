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
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;

public class RunMiNiFiTest {

    @Test
    public void buildSecurityPropertiesNotDefined() throws Exception {
        final RunMiNiFi testMiNiFi = new RunMiNiFi(null);
        final Properties bootstrapProperties = getTestBootstrapProperties("bootstrap-ssl-ctx/bootstrap.conf.default");
        final Optional<SecurityPropertiesSchema> securityPropsOptional = BootstrapTransformer.buildSecurityPropertiesFromBootstrap(bootstrapProperties);
        Assert.assertTrue(!securityPropsOptional.isPresent());
    }

    @Test
    public void buildSecurityPropertiesDefined() throws Exception {
        final RunMiNiFi testMiNiFi = new RunMiNiFi(null);
        final Properties bootstrapProperties = getTestBootstrapProperties("bootstrap-ssl-ctx/bootstrap.conf.configured");
        final Optional<SecurityPropertiesSchema> securityPropsOptional = BootstrapTransformer.buildSecurityPropertiesFromBootstrap(bootstrapProperties);
        Assert.assertTrue(securityPropsOptional.isPresent());

        final SecurityPropertiesSchema securityPropertiesSchema = securityPropsOptional.get();
        Assert.assertEquals("/my/test/keystore.jks", securityPropertiesSchema.getKeystore());
        Assert.assertEquals("JKS", securityPropertiesSchema.getKeystoreType());
        Assert.assertEquals("mykeystorepassword", securityPropertiesSchema.getKeystorePassword());
        Assert.assertEquals("mykeypassword", securityPropertiesSchema.getKeyPassword());

        Assert.assertEquals("/my/test/truststore.jks", securityPropertiesSchema.getTruststore());
        Assert.assertEquals("JKS", securityPropertiesSchema.getTruststoreType());
        Assert.assertEquals("mytruststorepassword", securityPropertiesSchema.getTruststorePassword());

        Assert.assertEquals("TLS", securityPropertiesSchema.getSslProtocol());

        final SensitivePropsSchema sensitiveProps = securityPropertiesSchema.getSensitiveProps();
        Assert.assertNotNull(sensitiveProps);

        Assert.assertEquals("sensitivepropskey", sensitiveProps.getKey());
        Assert.assertEquals("algo", sensitiveProps.getAlgorithm());
        Assert.assertEquals("BC", sensitiveProps.getProvider());


        Assert.assertTrue(securityPropertiesSchema.isValid());
    }

    @Test
    public void buildSecurityPropertiesDefinedButInvalid() throws Exception {
        final RunMiNiFi testMiNiFi = new RunMiNiFi(null);
        final Properties bootstrapProperties = getTestBootstrapProperties("bootstrap-ssl-ctx/bootstrap.conf.configured.invalid");
        final Optional<SecurityPropertiesSchema> securityPropsOptional = BootstrapTransformer.buildSecurityPropertiesFromBootstrap(bootstrapProperties);
        Assert.assertTrue(securityPropsOptional.isPresent());

        final SecurityPropertiesSchema securityPropertiesSchema = securityPropsOptional.get();
        Assert.assertEquals("/my/test/keystore.jks", securityPropertiesSchema.getKeystore());
        Assert.assertEquals("NOTAKEYSTORETYPE", securityPropertiesSchema.getKeystoreType());
        Assert.assertEquals("mykeystorepassword", securityPropertiesSchema.getKeystorePassword());
        Assert.assertEquals("mykeypassword", securityPropertiesSchema.getKeyPassword());

        Assert.assertEquals("/my/test/truststore.jks", securityPropertiesSchema.getTruststore());
        Assert.assertEquals("JKS", securityPropertiesSchema.getTruststoreType());
        Assert.assertEquals("mytruststorepassword", securityPropertiesSchema.getTruststorePassword());

        final SensitivePropsSchema sensitiveProps = securityPropertiesSchema.getSensitiveProps();
        Assert.assertNotNull(sensitiveProps);

        Assert.assertEquals("sensitivepropskey", sensitiveProps.getKey());
        Assert.assertEquals("algo", sensitiveProps.getAlgorithm());
        Assert.assertEquals("BC", sensitiveProps.getProvider());

        Assert.assertFalse(securityPropertiesSchema.isValid());

    }

    @Test
    public void buildProvenanceReportingNotDefined() throws Exception {
        final RunMiNiFi testMiNiFi = new RunMiNiFi(null);
        final Properties bootstrapProperties = getTestBootstrapProperties("bootstrap-provenance-reporting/bootstrap.conf.default");
        final Optional<ProvenanceReportingSchema> provenanceReportingPropsOptional = BootstrapTransformer.buildProvenanceReportingPropertiesFromBootstrap(bootstrapProperties);
        Assert.assertTrue(!provenanceReportingPropsOptional.isPresent());
    }

    @Test
    public void buildProvenanceReportingDefined() throws Exception {
        final RunMiNiFi testMiNiFi = new RunMiNiFi(null);
        final Properties bootstrapProperties = getTestBootstrapProperties("bootstrap-provenance-reporting/bootstrap.conf.configured");
        final Optional<ProvenanceReportingSchema> provenanceReportingPropsOptional = BootstrapTransformer.buildProvenanceReportingPropertiesFromBootstrap(bootstrapProperties);
        Assert.assertTrue(provenanceReportingPropsOptional.isPresent());

        final ProvenanceReportingSchema provenanceReportingSchema = provenanceReportingPropsOptional.get();
        Assert.assertEquals("This is a comment!", provenanceReportingSchema.getComment());
        Assert.assertEquals("TIMER_DRIVEN", provenanceReportingSchema.getSchedulingStrategy());
        Assert.assertEquals("15 secs", provenanceReportingSchema.getSchedulingPeriod());
        Assert.assertEquals("http://localhost:8080/", provenanceReportingSchema.getDestinationUrl());
        Assert.assertEquals("provenance", provenanceReportingSchema.getPortName());
        Assert.assertEquals("http://${hostname(true)}:8081/nifi", provenanceReportingSchema.getOriginatingUrl());
        Assert.assertEquals("10 secs", provenanceReportingSchema.getTimeout());
    }


    public static Properties getTestBootstrapProperties(final String fileName) throws IOException {
        final Properties bootstrapProperties = new Properties();
        try (final InputStream fis = RunMiNiFiTest.class.getClassLoader().getResourceAsStream(fileName)) {
            bootstrapProperties.load(fis);
        }
        return bootstrapProperties;
    }

}