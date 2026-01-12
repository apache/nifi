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

import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.dbcp.ConnectionUrlValidator;
import org.apache.nifi.dbcp.DBCPValidator;
import org.apache.nifi.dbcp.DriverClassValidator;
import org.apache.nifi.dbcp.api.DatabasePasswordProvider;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.concurrent.TimeUnit;

public final class DBCPProperties {

    private DBCPProperties() {
    }

    public static final List<String> OLD_DB_DRIVER_LOCATION_PROPERTY_NAMES = List.of(
            "database-driver-locations",
            "Database Driver Location(s)"
    );
    public static final String OLD_VALIDATION_QUERY_PROPERTY_NAME = "Validation-query";
    public static final String OLD_MIN_IDLE_PROPERTY_NAME = "dbcp-min-idle-conns";
    public static final String OLD_MAX_IDLE_PROPERTY_NAME = "dbcp-max-idle-conns";
    public static final String OLD_MAX_CONN_LIFETIME_PROPERTY_NAME = "dbcp-max-conn-lifetime";
    public static final String OLD_EVICTION_RUN_PERIOD_PROPERTY_NAME = "dbcp-time-between-eviction-runs";
    public static final String OLD_MIN_EVICTABLE_IDLE_TIME_PROPERTY_NAME = "dbcp-min-evictable-idle-time";
    public static final String OLD_SOFT_MIN_EVICTABLE_IDLE_TIME_PROPERTY_NAME = "dbcp-soft-min-evictable-idle-time";
    public static final String OLD_KERBEROS_USER_SERVICE_PROPERTY_NAME = "kerberos-user-service";

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

    public static final PropertyDescriptor PASSWORD_SOURCE = new PropertyDescriptor.Builder()
            .name("Password Source")
            .description("Specifies whether to supply the database password directly or obtain it from a Database Password Provider.")
            .allowableValues(PasswordSource.class)
            .defaultValue(PasswordSource.PASSWORD)
            .required(true)
            .build();

    public static final PropertyDescriptor DB_PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("The password for the database user")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor DB_PASSWORD_WITH_PASSWORD_SOURCE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(DB_PASSWORD)
            .dependsOn(PASSWORD_SOURCE, PasswordSource.PASSWORD)
            .build();

    public static final PropertyDescriptor DB_PASSWORD_PROVIDER = new PropertyDescriptor.Builder()
            .name("Database Password Provider")
            .description("Controller Service that supplies database passwords on demand. When configured, the Password property is ignored.")
            .required(true)
            .identifiesControllerService(DatabasePasswordProvider.class)
            .dependsOn(PASSWORD_SOURCE, PasswordSource.PASSWORD_PROVIDER)
            .build();

    public static final PropertyDescriptor DB_DRIVERNAME = new PropertyDescriptor.Builder()
            .name("Database Driver Class Name")
            .description("Database driver class name")
            .required(true)
            .addValidator(new DriverClassValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor DB_DRIVER_LOCATION = new PropertyDescriptor.Builder()
            .name("Database Driver Locations")
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
            .name("Validation Query")
            .description("Validation query used to validate connections before returning them. "
                    + "When connection is invalid, it gets dropped and new valid connection will be returned. "
                    + "Note!! Using validation might have some performance penalty.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public enum PasswordSource implements DescribedValue {
        PASSWORD("Password", "Use the configured Password property for database authentication."),
        PASSWORD_PROVIDER("Password Provider", "Obtain database passwords from a configured Database Password Provider.");

        private final String displayName;
        private final String description;

        PasswordSource(final String displayName, final String description) {
            this.displayName = displayName;
            this.description = description;
        }

        @Override
        public String getDisplayName() {
            return displayName;
        }

        @Override
        public String getValue() {
            return name();
        }

        @Override
        public String getDescription() {
            return description;
        }
    }

    public static final PropertyDescriptor MIN_IDLE = new PropertyDescriptor.Builder()
            .name("Minimum Idle Connections")
            .description("The minimum number of connections that can remain idle in the pool without extra ones being " +
                    "created. Set to or zero to allow no idle connections.")
            .defaultValue(DefaultDataSourceValues.MIN_IDLE.getValue())
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor MAX_IDLE = new PropertyDescriptor.Builder()
            .name("Maximum Idle Connections")
            .description("The maximum number of connections that can remain idle in the pool without extra ones being " +
                    "released. Set to any negative value to allow unlimited idle connections.")
            .defaultValue(DefaultDataSourceValues.MAX_IDLE.getValue())
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor MAX_CONN_LIFETIME = new PropertyDescriptor.Builder()
            .name("Maximum Connection Lifetime")
            .description("The maximum lifetime of a connection. After this time is exceeded the " +
                    "connection will fail the next activation, passivation or validation test. A value of zero or less " +
                    "means the connection has an infinite lifetime.")
            .defaultValue(DefaultDataSourceValues.MAX_CONN_LIFETIME.getValue())
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor EVICTION_RUN_PERIOD = new PropertyDescriptor.Builder()
            .name("Time Between Eviction Runs")
            .description("The time period to sleep between runs of the idle connection evictor thread. When " +
                    "non-positive, no idle connection evictor thread will be run.")
            .defaultValue(DefaultDataSourceValues.EVICTION_RUN_PERIOD.getValue())
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor MIN_EVICTABLE_IDLE_TIME = new PropertyDescriptor.Builder()
            .name("Minimum Evictable Idle Time")
            .description("The minimum amount of time a connection may sit idle in the pool before it is eligible for eviction.")
            .defaultValue(DefaultDataSourceValues.MIN_EVICTABLE_IDLE_TIME.getValue())
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor SOFT_MIN_EVICTABLE_IDLE_TIME = new PropertyDescriptor.Builder()
            .name("Soft Minimum Evictable Idle Time")
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
            .name("Kerberos User Service")
            .description("Specifies the Kerberos User Controller Service that should be used for authenticating with Kerberos")
            .identifiesControllerService(KerberosUserService.class)
            .required(false)
            .build();

    public static Long extractMillisWithInfinite(PropertyValue prop) {
        return "-1".equals(prop.getValue()) ? -1 : prop.asTimePeriod(TimeUnit.MILLISECONDS);
    }
}
