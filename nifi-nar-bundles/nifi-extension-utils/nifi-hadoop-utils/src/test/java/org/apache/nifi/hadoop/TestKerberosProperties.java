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

        // no security enabled in config so doesn't matter what principal and keytab are
        List<ValidationResult> results = KerberosProperties.validatePrincipalAndKeytab(
                "test", config, null, null, log);
        Assert.assertEquals(0, results.size());

        results = KerberosProperties.validatePrincipalAndKeytab(
                "test", config, "principal", null, log);
        Assert.assertEquals(0, results.size());

        results = KerberosProperties.validatePrincipalAndKeytab(
                "test", config, "principal", "keytab", log);
        Assert.assertEquals(0, results.size());

        // change the config to have kerberos turned on
        config.set("hadoop.security.authentication", "kerberos");
        config.set("hadoop.security.authorization", "true");

        results = KerberosProperties.validatePrincipalAndKeytab(
                "test", config, null, null, log);
        Assert.assertEquals(2, results.size());
    }

}
