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
package org.apache.nifi.dbcp.utils;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.dbcp.ConnectionUrlValidator;
import org.apache.nifi.dbcp.DBCPValidator;
import org.apache.nifi.dbcp.DriverClassValidator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.concurrent.TimeUnit;

public final class DBCPProperties {

    private DBCPProperties() {
    }

    public static final PropertyDescriptor DATABASE_URL = new PropertyDescriptor.Builder()
            .name("Database Connection URL")
            .description("A database connection URL used to connect to a database. May contain database system name, host, port, database name and some parameters."
                    + " The exact syntax of a database connection URL is specified by your DBMS.")
            .addValidator(new ConnectionUrlValidator())
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor DB_USER = new PropertyDescriptor.Builder()
            .name("Database User")
            .description("Database user name")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor DB_PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("The password for the database user")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();


    public static final PropertyDescriptor DB_DRIVERNAME = new PropertyDescriptor.Builder()
            .name("Database Driver Class Name")
            .description("Database driver class name")
            .required(true)
            .addValidator(new DriverClassValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor DB_DRIVER_LOCATION = new PropertyDescriptor.Builder()
            .name("database-driver-locations")
            .displayName("Database Driver Location(s)")
            .description("Comma-separated list of files/folders and/or URLs containing the driver JAR and its dependencies (if any). For example '/var/tmp/mariadb-java-client-1.1.7.jar'")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE, ResourceType.DIRECTORY, ResourceType.URL)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dynamicallyModifiesClasspath(true)
            .build();

    public static final PropertyDescriptor MAX_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("Max Wait Time")
            .description("The maximum amount of time that the pool will wait (when there are no available connections) "
                    + " for a connection to be returned before failing, or -1 to wait indefinitely. ")
            .defaultValue(DefaultDataSourceValues.MAX_WAIT_TIME.getValue())
            .required(true)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor MAX_TOTAL_CONNECTIONS = new PropertyDescriptor.Builder()
            .name("Max Total Connections")
            .description("The maximum number of active connections that can be allocated from this pool at the same time, "
                    + " or negative for no limit.")
            .defaultValue(DefaultDataSourceValues.MAX_TOTAL_CONNECTIONS.getValue())
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor VALIDATION_QUERY = new PropertyDescriptor.Builder()
            .name("Validation-query")
            .displayName("Validation query")
            .description("Validation query used to validate connections before returning them. "
                    + "When connection is invalid, it gets dropped and new valid connection will be returned. "
                    + "Note!! Using validation might have some performance penalty.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor MIN_IDLE = new PropertyDescriptor.Builder()
            .displayName("Minimum Idle Connections")
            .name("dbcp-min-idle-conns")
            .description("The minimum number of connections that can remain idle in the pool without extra ones being " +
                    "created. Set to or zero to allow no idle connections.")
            .defaultValue(DefaultDataSourceValues.MIN_IDLE.getValue())
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor MAX_IDLE = new PropertyDescriptor.Builder()
            .displayName("Max Idle Connections")
            .name("dbcp-max-idle-conns")
            .description("The maximum number of connections that can remain idle in the pool without extra ones being " +
                    "released. Set to any negative value to allow unlimited idle connections.")
            .defaultValue(DefaultDataSourceValues.MAX_IDLE.getValue())
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor MAX_CONN_LIFETIME = new PropertyDescriptor.Builder()
            .displayName("Max Connection Lifetime")
            .name("dbcp-max-conn-lifetime")
            .description("The maximum lifetime of a connection. After this time is exceeded the " +
                    "connection will fail the next activation, passivation or validation test. A value of zero or less " +
                    "means the connection has an infinite lifetime.")
            .defaultValue(DefaultDataSourceValues.MAX_CONN_LIFETIME.getValue())
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor EVICTION_RUN_PERIOD = new PropertyDescriptor.Builder()
            .displayName("Time Between Eviction Runs")
            .name("dbcp-time-between-eviction-runs")
            .description("The time period to sleep between runs of the idle connection evictor thread. When " +
                    "non-positive, no idle connection evictor thread will be run.")
            .defaultValue(DefaultDataSourceValues.EVICTION_RUN_PERIOD.getValue())
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor MIN_EVICTABLE_IDLE_TIME = new PropertyDescriptor.Builder()
            .displayName("Minimum Evictable Idle Time")
            .name("dbcp-min-evictable-idle-time")
            .description("The minimum amount of time a connection may sit idle in the pool before it is eligible for eviction.")
            .defaultValue(DefaultDataSourceValues.MIN_EVICTABLE_IDLE_TIME.getValue())
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor SOFT_MIN_EVICTABLE_IDLE_TIME = new PropertyDescriptor.Builder()
            .displayName("Soft Minimum Evictable Idle Time")
            .name("dbcp-soft-min-evictable-idle-time")
            .description("The minimum amount of time a connection may sit idle in the pool before it is eligible for " +
                    "eviction by the idle connection evictor, with the extra condition that at least a minimum number of" +
                    " idle connections remain in the pool. When the not-soft version of this option is set to a positive" +
                    " value, it is examined first by the idle connection evictor: when idle connections are visited by " +
                    "the evictor, idle time is first compared against it (without considering the number of idle " +
                    "connections in the pool) and then against this soft option, including the minimum idle connections " +
                    "constraint.")
            .defaultValue(DefaultDataSourceValues.SOFT_MIN_EVICTABLE_IDLE_TIME.getValue())
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor KERBEROS_USER_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-user-service")
            .displayName("Kerberos User Service")
            .description("Specifies the Kerberos User Controller Service that should be used for authenticating with Kerberos")
            .identifiesControllerService(KerberosUserService.class)
            .required(false)
            .build();

    public static Long extractMillisWithInfinite(PropertyValue prop) {
        return "-1".equals(prop.getValue()) ? -1 : prop.asTimePeriod(TimeUnit.MILLISECONDS);
    }
}
