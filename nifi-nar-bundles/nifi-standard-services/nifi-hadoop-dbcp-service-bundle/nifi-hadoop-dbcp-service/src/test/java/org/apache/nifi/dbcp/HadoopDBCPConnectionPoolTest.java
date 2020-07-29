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
package org.apache.nifi.dbcp;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.kerberos.KerberosContext;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockKerberosContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class HadoopDBCPConnectionPoolTest {

    private File krbConfFile;
    private KerberosProperties kerberosProps;
    private KerberosContext kerberosContext;

    @Before
    public void setup() {
        krbConfFile = new File("src/test/resources/krb5.conf");
        kerberosProps = new KerberosProperties(krbConfFile);
        kerberosContext = new MockKerberosContext(krbConfFile);
    }

    @Test
    public void testCustomValidateWhenAllowExplicitKeytab() throws InitializationException {
        final Processor testProcessor = new TestProcessor();
        final TestRunner runner = TestRunners.newTestRunner(testProcessor, kerberosContext);

        // Configure minimum required properties..
        final HadoopDBCPConnectionPool hadoopDBCPService = new TestableHadoopDBCPConnectionPool(true);
        runner.addControllerService("hadoop-dbcp-service", hadoopDBCPService);
        runner.setProperty(hadoopDBCPService, HadoopDBCPConnectionPool.DATABASE_URL, "jdbc:phoenix:zk-host1,zk-host2:2181:/hbase");
        runner.setProperty(hadoopDBCPService, HadoopDBCPConnectionPool.DB_DRIVERNAME, "org.apache.phoenix.jdbc.PhoenixDriver");
        runner.setProperty(hadoopDBCPService, HadoopDBCPConnectionPool.DB_DRIVER_LOCATION, "target");

        // Security is not enabled yet since no conf files provided, so should be valid
        runner.assertValid(hadoopDBCPService);

        // Enable security, should be invalid until some form of kerberos credentials are provided
        runner.setProperty(hadoopDBCPService, HadoopDBCPConnectionPool.HADOOP_CONFIGURATION_RESOURCES, "src/test/resources/core-site-security.xml");
        runner.assertNotValid(hadoopDBCPService);

        // Configure principal and keytab, should be valid
        runner.setProperty(hadoopDBCPService, kerberosProps.getKerberosPrincipal(), "nifi@EXAMPLE.COM");
        runner.setProperty(hadoopDBCPService, kerberosProps.getKerberosKeytab(), "src/test/resources/fake.keytab");
        runner.assertValid(hadoopDBCPService);

        // Configure password, should become invalid
        runner.setProperty(hadoopDBCPService, kerberosProps.getKerberosPassword(), "password");
        runner.assertNotValid(hadoopDBCPService);

        // Remove keytab property, should become valid
        runner.removeProperty(hadoopDBCPService, kerberosProps.getKerberosKeytab());
        runner.assertValid(hadoopDBCPService);

        // Configure a KeberosCredentialService, should become invalid
        final KerberosCredentialsService kerberosCredentialsService = new MockKerberosCredentialsService(
                "nifi@EXAMPLE.COM", "src/test/resources/fake.keytab");
        runner.addControllerService("kerb-credentials", kerberosCredentialsService);
        runner.enableControllerService(kerberosCredentialsService);
        runner.setProperty(hadoopDBCPService, HadoopDBCPConnectionPool.KERBEROS_CREDENTIALS_SERVICE, "kerb-credentials");
        runner.assertNotValid(hadoopDBCPService);

        // Remove password property, still invalid
        runner.removeProperty(hadoopDBCPService, kerberosProps.getKerberosPassword());
        runner.assertNotValid(hadoopDBCPService);

        // Remove principal property, only using keytab service, should become valid
        runner.removeProperty(hadoopDBCPService, kerberosProps.getKerberosPrincipal());
        runner.assertValid(hadoopDBCPService);
    }

    @Test
    public void testCustomValidateWhenNotAllowExplicitKeytab() throws InitializationException {
        final Processor testProcessor = new TestProcessor();
        final TestRunner runner = TestRunners.newTestRunner(testProcessor, kerberosContext);

        // Configure minimum required properties..
        final HadoopDBCPConnectionPool hadoopDBCPService = new TestableHadoopDBCPConnectionPool(false);
        runner.addControllerService("hadoop-dbcp-service", hadoopDBCPService);
        runner.setProperty(hadoopDBCPService, HadoopDBCPConnectionPool.DATABASE_URL, "jdbc:phoenix:zk-host1,zk-host2:2181:/hbase");
        runner.setProperty(hadoopDBCPService, HadoopDBCPConnectionPool.DB_DRIVERNAME, "org.apache.phoenix.jdbc.PhoenixDriver");
        runner.setProperty(hadoopDBCPService, HadoopDBCPConnectionPool.DB_DRIVER_LOCATION, "target");

        // Security is not enabled yet since no conf files provided, so should be valid
        runner.assertValid(hadoopDBCPService);

        // Enable security, should be invalid until some form of kerberos credentials are provided
        runner.setProperty(hadoopDBCPService, HadoopDBCPConnectionPool.HADOOP_CONFIGURATION_RESOURCES, "src/test/resources/core-site-security.xml");
        runner.assertNotValid(hadoopDBCPService);

        // Configure principal and keytab, should be valid
        runner.setProperty(hadoopDBCPService, kerberosProps.getKerberosPrincipal(), "nifi@EXAMPLE.COM");
        runner.assertNotValid(hadoopDBCPService);
    }

    private static final class TestableHadoopDBCPConnectionPool extends HadoopDBCPConnectionPool {

        private final boolean allowExplicitKeytab;

        public TestableHadoopDBCPConnectionPool(boolean allowExplicitKeytab) {
            this.allowExplicitKeytab = allowExplicitKeytab;
        }

        @Override
        boolean isAllowExplicitKeytab() {
            return allowExplicitKeytab;
        }
    }

    private class MockKerberosCredentialsService extends AbstractControllerService implements KerberosCredentialsService {

        private String principal;
        private String keytab;

        public MockKerberosCredentialsService(String principal, String keytab) {
            this.principal = principal;
            this.keytab = keytab;
        }

        @Override
        public String getKeytab() {
            return keytab;
        }

        @Override
        public String getPrincipal() {
            return principal;
        }
    }

}
