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
package org.apache.nifi.snowflake.service;

import net.snowflake.client.jdbc.SnowflakeDriver;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.DBCPConnectionPool;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;

import java.sql.Driver;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Implementation of Database Connection Pooling Service for Snowflake.
 * Apache DBCP is used for connection pooling functionality.
 */
@Tags({"snowflake", "dbcp", "jdbc", "database", "connection", "pooling", "store"})
@CapabilityDescription("Provides Snowflake Connection Pooling Service. Connections can be asked from pool and returned after usage.")
@DynamicProperties({
    @DynamicProperty(name = "JDBC property name",
        value = "Snowflake JDBC property value",
        expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY,
        description = "Snowflake JDBC driver property name and value applied to JDBC connections."),
    @DynamicProperty(name = "SENSITIVE.JDBC property name",
        value = "Snowflake JDBC property value",
        expressionLanguageScope = ExpressionLanguageScope.NONE,
        description = "Snowflake JDBC driver property name prefixed with 'SENSITIVE.' handled as a sensitive property.")
})
@RequiresInstanceClassLoading
public class SnowflakeComputingConnectionPool extends DBCPConnectionPool {

    public static final PropertyDescriptor SNOWFLAKE_URL = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(DBCPConnectionPool.DATABASE_URL)
        .displayName("Snowflake URL")
        .description("Example connection string: jdbc:snowflake://[account].[region].snowflakecomputing.com/?[connection_params]" +
            " The connection parameters can include db=DATABASE_NAME to avoid using qualified table names such as DATABASE_NAME.PUBLIC.TABLE_NAME")
        .build();

    public static final PropertyDescriptor SNOWFLAKE_USER = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(DBCPConnectionPool.DB_USER)
        .displayName("Snowflake User")
        .description("The Snowflake user name")
        .build();

    public static final PropertyDescriptor SNOWFLAKE_PASSWORD = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(DBCPConnectionPool.DB_PASSWORD)
        .displayName("Snowflake Password")
        .description("The password for the Snowflake user")
        .build();

    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(SNOWFLAKE_URL);
        props.add(SNOWFLAKE_USER);
        props.add(SNOWFLAKE_PASSWORD);
        props.add(VALIDATION_QUERY);
        props.add(MAX_WAIT_TIME);
        props.add(MAX_TOTAL_CONNECTIONS);
        props.add(MIN_IDLE);
        props.add(MAX_IDLE);
        props.add(MAX_CONN_LIFETIME);
        props.add(EVICTION_RUN_PERIOD);
        props.add(MIN_EVICTABLE_IDLE_TIME);
        props.add(SOFT_MIN_EVICTABLE_IDLE_TIME);

        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        return Collections.emptyList();
    }

    @Override
    protected String getUrl(final ConfigurationContext context) {
        String snowflakeUrl = context.getProperty(SNOWFLAKE_URL).evaluateAttributeExpressions().getValue();

        if (!snowflakeUrl.startsWith("jdbc:snowflake")) {
            snowflakeUrl = "jdbc:snowflake://" + snowflakeUrl;
        }

        return snowflakeUrl;
    }

    @Override
    protected Driver getDriver(final String driverName, final String url) {
        try {
            Class.forName(SnowflakeDriver.class.getName());
            return DriverManager.getDriver(url);
        } catch (Exception e) {
            throw new ProcessException("Snowflake driver unavailable or incompatible connection URL", e);
        }
    }
}
