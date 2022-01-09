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
package org.apache.nifi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.logging.ComponentLog;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.List;

public class TestKerberosProperties {

    @Test
    public void testWithKerberosConfigFile() {
        final File file = new File("src/test/resources/krb5.conf");

        final KerberosProperties kerberosProperties = new KerberosProperties(file);
        Assert.assertNotNull(kerberosProperties);

        Assert.assertNotNull(kerberosProperties.getKerberosConfigFile());
        Assert.assertNotNull(kerberosProperties.getKerberosConfigValidator());
        Assert.assertNotNull(kerberosProperties.getKerberosPrincipal());
        Assert.assertNotNull(kerberosProperties.getKerberosKeytab());

        final ValidationResult result = kerberosProperties.getKerberosConfigValidator().validate("test", "principal", null);
        Assert.assertTrue(result.isValid());
    }

    @Test
    public void testWithoutKerberosConfigFile() {
        final File file = new File("src/test/resources/krb5.conf");

        final KerberosProperties kerberosProperties = new KerberosProperties(null);
        Assert.assertNotNull(kerberosProperties);

        Assert.assertNull(kerberosProperties.getKerberosConfigFile());
        Assert.assertNotNull(kerberosProperties.getKerberosConfigValidator());
        Assert.assertNotNull(kerberosProperties.getKerberosPrincipal());
        Assert.assertNotNull(kerberosProperties.getKerberosKeytab());

        final ValidationResult result = kerberosProperties.getKerberosConfigValidator().validate("test", "principal", null);
        Assert.assertFalse(result.isValid());
    }

    @Test
    public void testValidatePrincipalAndKeytab() {
        final ComponentLog log = Mockito.mock(ComponentLog.class);
        final Configuration config = new Configuration();

        // no security enabled in config so doesn't matter what principal, keytab, and password are
        List<ValidationResult> results = KerberosProperties.validatePrincipalWithKeytabOrPassword(
                "test", config, null, null, null, log);
        Assert.assertEquals(0, results.size());

        results = KerberosProperties.validatePrincipalWithKeytabOrPassword(
                "test", config, "principal", null, null, log);
        Assert.assertEquals(0, results.size());

        results = KerberosProperties.validatePrincipalWithKeytabOrPassword(
                "test", config, "principal", "keytab", null, log);
        Assert.assertEquals(0, results.size());

        results = KerberosProperties.validatePrincipalWithKeytabOrPassword(
                "test", config, "principal", null, "password", log);
        Assert.assertEquals(0, results.size());

        results = KerberosProperties.validatePrincipalWithKeytabOrPassword(
                "test", config, "principal", "keytab", "password", log);
        Assert.assertEquals(0, results.size());

        // change the config to have kerberos turned on
        config.set("hadoop.security.authentication", "kerberos");
        config.set("hadoop.security.authorization", "true");

        // security is enabled, no principal, keytab, or password provided
        results = KerberosProperties.validatePrincipalWithKeytabOrPassword(
                "test", config, null, null, null, log);
        Assert.assertEquals(2, results.size());

        // security is enabled, keytab provided, no principal or password provided
        results = KerberosProperties.validatePrincipalWithKeytabOrPassword(
                "test", config, null, "keytab", null, log);
        Assert.assertEquals(1, results.size());

        // security is enabled, password provided, no principal or keytab provided
        results = KerberosProperties.validatePrincipalWithKeytabOrPassword(
                "test", config, null, null, "password", log);
        Assert.assertEquals(1, results.size());

        // security is enabled, no principal provided, keytab and password provided
        results = KerberosProperties.validatePrincipalWithKeytabOrPassword(
                "test", config, null, "keytab", "password", log);
        Assert.assertEquals(2, results.size());

        // security is enabled, principal provided, no keytab or password provided
        results = KerberosProperties.validatePrincipalWithKeytabOrPassword(
                "test", config, "principal", null, null, log);
        Assert.assertEquals(1, results.size());

        // security is enabled, principal and keytab provided, no password provided
        results = KerberosProperties.validatePrincipalWithKeytabOrPassword(
                "test", config, "principal", "keytab", null, log);
        Assert.assertEquals(0, results.size());

        // security is enabled, no keytab provided, principal and password provided
        results = KerberosProperties.validatePrincipalWithKeytabOrPassword(
                "test", config, "principal", null, "password", log);
        Assert.assertEquals(0, results.size());

        // security is enabled, principal, keytab, and password provided
        results = KerberosProperties.validatePrincipalWithKeytabOrPassword(
                "test", config, "principal", "keytab", "password", log);
        Assert.assertEquals(1, results.size());
    }

}
