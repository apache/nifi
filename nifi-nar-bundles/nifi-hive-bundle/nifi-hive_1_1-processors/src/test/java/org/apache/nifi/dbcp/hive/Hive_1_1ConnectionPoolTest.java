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

package org.apache.nifi.dbcp.hive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockControllerServiceLookup;
import org.apache.nifi.util.MockVariableRegistry;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class Hive_1_1ConnectionPoolTest {
    private UserGroupInformation userGroupInformation;
    private Hive_1_1ConnectionPool hiveConnectionPool;
    private BasicDataSource basicDataSource;
    private ComponentLog componentLog;
    private final File krb5conf = new File("src/test/resources/krb5.conf");
    private TestRunner runner;
    private static final String DB_LOCATION = "target/db";

    @BeforeClass
    public static void setupBeforeClass() {
        System.setProperty("derby.stream.error.file", "target/derby.log");
    }

    @Before
    public void setup() throws Exception {
        // have to initialize this system property before anything else
        System.setProperty("java.security.krb5.conf", krb5conf.getAbsolutePath());
        System.setProperty("java.security.krb5.realm", "nifi.com");
        System.setProperty("java.security.krb5.kdc", "nifi.kdc");

        userGroupInformation = mock(UserGroupInformation.class);
        basicDataSource = mock(BasicDataSource.class);
        componentLog = mock(ComponentLog.class);

        when(userGroupInformation.doAs(isA(PrivilegedExceptionAction.class))).thenAnswer(invocation -> {
            try {
                return ((PrivilegedExceptionAction) invocation.getArguments()[0]).run();
            } catch (IOException | Error | RuntimeException | InterruptedException e) {
                throw e;
            } catch (Throwable e) {
                throw new UndeclaredThrowableException(e);
            }
        });

        runner = TestRunners.newTestRunner(MockProcessor.class);
        initPool();
        runner.addControllerService("1", hiveConnectionPool);
    }

    private void initPool() throws Exception {
        hiveConnectionPool = new Hive_1_1ConnectionPoolOverride();

        Field ugiField = Hive_1_1ConnectionPool.class.getDeclaredField("ugi");
        ugiField.setAccessible(true);
        ugiField.set(hiveConnectionPool, userGroupInformation);

        Field dataSourceField = Hive_1_1ConnectionPool.class.getDeclaredField("dataSource");
        dataSourceField.setAccessible(true);
        dataSourceField.set(hiveConnectionPool, basicDataSource);

        Field componentLogField = AbstractControllerService.class.getDeclaredField("logger");
        componentLogField.setAccessible(true);
        componentLogField.set(hiveConnectionPool, componentLog);
    }

    @Test(expected = ProcessException.class)
    public void testGetConnectionSqlException() throws SQLException {
        SQLException sqlException = new SQLException("bad sql");
        when(basicDataSource.getConnection()).thenThrow(sqlException);
        try {
            hiveConnectionPool.getConnection();
        } catch (ProcessException e) {
            assertEquals(sqlException, e.getCause());
            throw e;
        }
    }

    @Test
    public void testExpressionLanguageSupport() throws Exception {
        final String URL = "jdbc:hive2://localhost:10000/default";
        final String USER = "user";
        final String PASS = "pass";
        final int MAX_CONN = 7;
        final String MAX_CONN_LIFETIME = "1 sec";
        final String MAX_WAIT = "10 sec"; // 10000 milliseconds
        final String CONF = "/path/to/hive-site.xml";
        hiveConnectionPool = new Hive_1_1ConnectionPoolOverride();

        Map<PropertyDescriptor, String> props = new HashMap<PropertyDescriptor, String>() {{
            put(Hive_1_1ConnectionPool.DATABASE_URL, "${url}");
            put(Hive_1_1ConnectionPool.DB_USER, "${username}");
            put(Hive_1_1ConnectionPool.DB_PASSWORD, "${password}");
            put(Hive_1_1ConnectionPool.MAX_TOTAL_CONNECTIONS, "${maxconn}");
            put(Hive_1_1ConnectionPool.MAX_CONN_LIFETIME, "${maxconnlifetime}");
            put(Hive_1_1ConnectionPool.MAX_WAIT_TIME, "${maxwait}");
            put(Hive_1_1ConnectionPool.HIVE_CONFIGURATION_RESOURCES, "${hiveconf}");
        }};

        MockVariableRegistry registry = new MockVariableRegistry();
        registry.setVariable(new VariableDescriptor("url"), URL);
        registry.setVariable(new VariableDescriptor("username"), USER);
        registry.setVariable(new VariableDescriptor("password"), PASS);
        registry.setVariable(new VariableDescriptor("maxconn"), Integer.toString(MAX_CONN));
        registry.setVariable(new VariableDescriptor("maxconnlifetime"), MAX_CONN_LIFETIME);
        registry.setVariable(new VariableDescriptor("maxwait"), MAX_WAIT);
        registry.setVariable(new VariableDescriptor("hiveconf"), CONF);


        MockConfigurationContext context = new MockConfigurationContext(props, null, registry);
        hiveConnectionPool.onConfigured(context);

        Field dataSourceField = Hive_1_1ConnectionPool.class.getDeclaredField("dataSource");
        dataSourceField.setAccessible(true);
        basicDataSource = (BasicDataSource) dataSourceField.get(hiveConnectionPool);
        assertEquals(URL, basicDataSource.getUrl());
        assertEquals(USER, basicDataSource.getUsername());
        assertEquals(PASS, basicDataSource.getPassword());
        assertEquals(MAX_CONN, basicDataSource.getMaxTotal());
        assertEquals(1000L, basicDataSource.getMaxConnLifetimeMillis());
        assertEquals(10000L, basicDataSource.getMaxWaitMillis());
        assertEquals(URL, hiveConnectionPool.getConnectionURL());
    }

    @Test
    public void testDynamicProperties() throws InitializationException, SQLException {
        assertConnectionNotNullDynamicProperty("create", "true");
    }

    @Test
    public void testExpressionLanguageDynamicProperties() throws InitializationException, SQLException {
        assertConnectionNotNullDynamicProperty("create", "${literal(1):gt(0)}");
    }

    @Test
    public void testSensitiveDynamicProperties() throws InitializationException, SQLException {
        assertConnectionNotNullDynamicProperty("SENSITIVE.create", "true");
    }

    private void assertConnectionNotNullDynamicProperty(final String propertyName, final String propertyValue) throws InitializationException, SQLException {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        final Hive_1_1ConnectionPool service = new Hive_1_1ConnectionPoolOverride();
        runner.addControllerService(Hive_1_1ConnectionPool.class.getSimpleName(), service);

        runner.setProperty(service, Hive_1_1ConnectionPool.DATABASE_URL, "jdbc:derby:" + DB_LOCATION);
        runner.setProperty(service, Hive_1_1ConnectionPool.DB_USER, "tester");
        runner.setProperty(service, Hive_1_1ConnectionPool.DB_PASSWORD, "testerp");
        runner.setProperty(service, propertyName, propertyValue);

        runner.enableControllerService(service);
        runner.assertValid(service);

        try (final Connection connection = service.getConnection()) {
            assertNotNull(connection);
        }
    }


    @Ignore("Kerberos does not seem to be properly handled in Travis build, but, locally, this test should successfully run")
    @Test(expected = InitializationException.class)
    public void testKerberosAuthException() throws Exception {
        final String URL = "jdbc:hive2://localhost:10000/default";
        final String conf = "src/test/resources/hive-site-security.xml";
        final String ktab = "src/test/resources/fake.keytab";
        final String kprinc = "bad@PRINCIPAL.COM";
        final String kerberosCredentialsServiceId = UUID.randomUUID().toString();

        Map<PropertyDescriptor, String> props = new HashMap<PropertyDescriptor, String>() {{
            put(Hive_1_1ConnectionPool.DATABASE_URL, "${url}");
            put(Hive_1_1ConnectionPool.HIVE_CONFIGURATION_RESOURCES, "${conf}");
            put(Hive_1_1ConnectionPool.KERBEROS_CREDENTIALS_SERVICE, kerberosCredentialsServiceId);
        }};

        MockVariableRegistry registry = new MockVariableRegistry();
        registry.setVariable(new VariableDescriptor("url"), URL);
        registry.setVariable(new VariableDescriptor("conf"), conf);

        MockControllerServiceLookup mockControllerServiceLookup = new MockControllerServiceLookup() {};
        KerberosCredentialsService kerberosCredentialsService = mock(KerberosCredentialsService.class);
        when(kerberosCredentialsService.getKeytab()).thenReturn(ktab);
        when(kerberosCredentialsService.getPrincipal()).thenReturn(kprinc);
        mockControllerServiceLookup.addControllerService(kerberosCredentialsService, kerberosCredentialsServiceId);

        MockConfigurationContext context = new MockConfigurationContext(props, mockControllerServiceLookup, registry);
        hiveConnectionPool.onConfigured(context);
    }

    @Test
    public void testValidWithDynamicProperty() throws Exception {
        // set embedded Derby database connection url
        runner.setProperty(hiveConnectionPool, Hive_1_1ConnectionPool.DATABASE_URL, "jdbc:derby:" + DB_LOCATION);
        runner.setProperty(hiveConnectionPool, Hive_1_1ConnectionPool.DB_USER, "tester");
        runner.setProperty(hiveConnectionPool, Hive_1_1ConnectionPool.DB_PASSWORD, "testerp");
        runner.setProperty(hiveConnectionPool, "create", "true");
        runner.assertValid(hiveConnectionPool);

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        runner.enableControllerService(hiveConnectionPool);

        Connection connection = hiveConnectionPool.getConnection();
        assertNotNull(connection);
        connection.close(); // will return connection to pool
        connection = hiveConnectionPool.getConnection();
        assertNotNull(connection);
        connection.close(); // will return connection to pool
    }

    public static class MockProcessor extends AbstractProcessor {
        static final PropertyDescriptor HIVE_CONNECTION_POOL = new PropertyDescriptor.Builder()
                .name("Hive Connection Pool")
                .required(true)
                .identifiesControllerService(Hive_1_1ConnectionPool.class)
                .build();

        static final List<PropertyDescriptor> PROPS = Collections.singletonList(HIVE_CONNECTION_POOL);

        @Override
        public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return PROPS;
        }

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        }
    }

    // Override the driver class name to use Derby
    private static class Hive_1_1ConnectionPoolOverride extends Hive_1_1ConnectionPool {
        @Override
        protected String getDriverClassName() {
            return "org.apache.derby.jdbc.EmbeddedDriver";
        }
    }
}
