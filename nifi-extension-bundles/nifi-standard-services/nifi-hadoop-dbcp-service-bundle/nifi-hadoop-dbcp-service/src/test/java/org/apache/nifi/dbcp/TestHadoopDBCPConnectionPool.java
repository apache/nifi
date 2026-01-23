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

import org.apache.nifi.dbcp.utils.DBCPProperties;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.apache.nifi.dbcp.utils.DBCPProperties.EVICTION_RUN_PERIOD;
import static org.apache.nifi.dbcp.utils.DBCPProperties.KERBEROS_USER_SERVICE;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MAX_CONN_LIFETIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MAX_IDLE;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MIN_EVICTABLE_IDLE_TIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MIN_IDLE;
import static org.apache.nifi.dbcp.utils.DBCPProperties.SOFT_MIN_EVICTABLE_IDLE_TIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.VALIDATION_QUERY;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHadoopDBCPConnectionPool {

    @Test
    void testMigrateProperties() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final HadoopDBCPConnectionPool service = new HadoopDBCPConnectionPool();
        runner.addControllerService("hadoopDBCPConnectionPool", service);

        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("hadoop-config-resources", HadoopDBCPConnectionPool.HADOOP_CONFIGURATION_RESOURCES.getName()),
                Map.entry("database-driver-locations", HadoopDBCPConnectionPool.DB_DRIVER_LOCATION.getName()),
                Map.entry("Database Driver Location(s)", HadoopDBCPConnectionPool.DB_DRIVER_LOCATION.getName()),
                Map.entry(DBCPProperties.OLD_VALIDATION_QUERY_PROPERTY_NAME, VALIDATION_QUERY.getName()),
                Map.entry(DBCPProperties.OLD_MIN_IDLE_PROPERTY_NAME, MIN_IDLE.getName()),
                Map.entry(DBCPProperties.OLD_MAX_IDLE_PROPERTY_NAME, MAX_IDLE.getName()),
                Map.entry(DBCPProperties.OLD_MAX_CONN_LIFETIME_PROPERTY_NAME, MAX_CONN_LIFETIME.getName()),
                Map.entry(DBCPProperties.OLD_EVICTION_RUN_PERIOD_PROPERTY_NAME, EVICTION_RUN_PERIOD.getName()),
                Map.entry(DBCPProperties.OLD_MIN_EVICTABLE_IDLE_TIME_PROPERTY_NAME, MIN_EVICTABLE_IDLE_TIME.getName()),
                Map.entry(DBCPProperties.OLD_SOFT_MIN_EVICTABLE_IDLE_TIME_PROPERTY_NAME, SOFT_MIN_EVICTABLE_IDLE_TIME.getName()),
                Map.entry(DBCPProperties.OLD_KERBEROS_USER_SERVICE_PROPERTY_NAME, KERBEROS_USER_SERVICE.getName())
        );

        final Map<String, String> propertyValues = Map.of();
        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(propertyValues);
        service.migrateProperties(configuration);

        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();
        final Map<String, String> propertiesRenamed = result.getPropertiesRenamed();

        assertEquals(expectedRenamed, propertiesRenamed);

        final Set<String> expectedRemoved = Set.of(
                "Kerberos Principal",
                "Kerberos Password",
                "Kerberos Keytab",
                "kerberos-credentials-service"
        );

        assertEquals(expectedRemoved, result.getPropertiesRemoved());
    }
}
