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
package org.apache.nifi.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.AbstractDBCPConnectionPool;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.nifi.service.JdbcUrlFormat.FULL_URL;
import static org.apache.nifi.service.JdbcUrlFormat.PARAMETERS;
import static org.apache.nifi.service.JdbcUrlFormat.POSTGRESQL_BASIC_URI;

/**
 * Implementation of Database Connection Pooling Service for PostgreSQL with built-in JDBC driver.
 * Apache DBCP is used for connection pooling functionality.
 */
@Tags({"postgres", "postgresql", "dbcp", "jdbc", "database", "connection", "pooling", "store"})
@CapabilityDescription("Provides PostgreSQL Connection Pooling Service with built-in JDBC driver." +
        " Connections can be requested from the pool and returned once they have been used.")
@RequiresInstanceClassLoading
public class PostgreSQLConnectionPool extends AbstractDBCPConnectionPool {

    public static final PropertyDescriptor CONNECTION_URL_FORMAT = new PropertyDescriptor.Builder()
            .name("connection-url-format")
            .displayName("Connection URL Format")
            .description("The format of the connection URL.")
            .allowableValues(JdbcUrlFormat.class)
            .required(true)
            .defaultValue(FULL_URL.getValue())
            .build();

    public static final PropertyDescriptor POSTGRES_DATABASE_URL = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(DATABASE_URL)
            .dependsOn(CONNECTION_URL_FORMAT, FULL_URL)
            .build();

    public static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
            .name("database-name")
            .displayName("Database Name")
            .description("The name of the database to connect.")
            .required(false)
            .dependsOn(CONNECTION_URL_FORMAT, PARAMETERS)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DATABASE_HOSTNAME = new PropertyDescriptor.Builder()
            .name("database-hostname")
            .displayName("Database Hostname")
            .description("The hostname of the database to connect.")
            .required(false)
            .dependsOn(CONNECTION_URL_FORMAT, PARAMETERS)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DATABASE_PORT = new PropertyDescriptor.Builder()
            .name("database-port")
            .displayName("Database Port")
            .description("The port of the database to connect.")
            .required(false)
            .dependsOn(CONNECTION_URL_FORMAT, PARAMETERS)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            CONNECTION_URL_FORMAT,
            POSTGRES_DATABASE_URL,
            DATABASE_NAME,
            DATABASE_HOSTNAME,
            DATABASE_PORT,
            DB_USER,
            DB_PASSWORD,
            KERBEROS_USER_SERVICE,
            MAX_WAIT_TIME,
            MAX_TOTAL_CONNECTIONS,
            VALIDATION_QUERY,
            MIN_IDLE,
            MAX_IDLE,
            MAX_CONN_LIFETIME,
            EVICTION_RUN_PERIOD,
            MIN_EVICTABLE_IDLE_TIME,
            SOFT_MIN_EVICTABLE_IDLE_TIME
    ));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected Driver getDriver(final String driverName, final String url) {
        try {
            return DriverManager.getDriver(url);
        } catch (Exception e) {
            throw new ProcessException("PostgreSQL driver unavailable or incompatible connection URL", e);
        }
    }

    @Override
    protected String getUrl(ConfigurationContext context) {
        final JdbcUrlFormat jdbcUrlFormat = JdbcUrlFormat.forName(context.getProperty(CONNECTION_URL_FORMAT).getValue());
        if (jdbcUrlFormat == FULL_URL) {
            return context.getProperty(POSTGRES_DATABASE_URL).evaluateAttributeExpressions().getValue();
        } else if (jdbcUrlFormat == PARAMETERS) {
            final String databaseName = context.getProperty(DATABASE_NAME).getValue();
            final String hostname = context.getProperty(DATABASE_HOSTNAME).getValue();
            final String port = context.getProperty(DATABASE_PORT).getValue();

            if (StringUtils.isNoneBlank(databaseName, hostname, port)) {
                return String.format(JdbcUrlFormat.POSTGRESQL_HOST_PORT_DB_URI_TEMPLATE, hostname, port, databaseName);
            } else if (StringUtils.isNoneBlank(hostname, port) && StringUtils.isBlank(databaseName)) {
                return String.format(JdbcUrlFormat.POSTGRESQL_HOST_PORT_URI_TEMPLATE, hostname, port);
            } else if (StringUtils.isNoneBlank(hostname, databaseName) && StringUtils.isBlank(port)) {
                return String.format(JdbcUrlFormat.POSTGRESQL_HOST_DB_URI_TEMPLATE, hostname, databaseName);
            } else if (StringUtils.isNotBlank(databaseName) && StringUtils.isAllBlank(hostname, port)) {
                return String.format(JdbcUrlFormat.POSTGRESQL_DB_URI_TEMPLATE, databaseName);
            } else if (StringUtils.isAllBlank(databaseName, hostname, port)) {
                return POSTGRESQL_BASIC_URI;
            }
        }
        throw new IllegalArgumentException("Invalid JDBC URI format");
    }
}