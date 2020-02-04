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
package org.apache.nifi.lookup.db;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class AbstractDatabaseLookupService extends AbstractControllerService {

    static final String KEY = "key";

    static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("dbrecord-lookup-dbcp-service")
            .displayName("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    static final PropertyDescriptor SQL_STATEMENT = new PropertyDescriptor.Builder()
            .name("dbrecord-lookup-sql-statement")
            .displayName("SQL statement")
            .description("The SQL statement to be used to query the database. This can be used alternative to Table Name.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("dbrecord-lookup-table-name")
            .displayName("Table Name")
            .description("The name of the database table to be queried. Note that this may be case-sensitive depending on the database.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor LOOKUP_KEY_COLUMN = new PropertyDescriptor.Builder()
            .name("dbrecord-lookup-key-column")
            .displayName("Lookup Key Column")
            .description("The column in the table that will serve as the lookup key. This is the column that will be matched against "
                    + "the property specified in the lookup processor. Note that this may be case-sensitive depending on the database.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("dbrecord-lookup-cache-size")
            .displayName("Cache Size")
            .description("Specifies how many lookup values/records should be cached. The cache is shared for all tables and keeps a map of lookup values to records. "
                    + "Setting this property to zero means no caching will be done and the table will be queried for each lookup value in each record. If the lookup "
                    + "table changes often or the most recent data must be retrieved, do not use the cache.")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("0")
            .required(true)
            .build();

    static final PropertyDescriptor CLEAR_CACHE_ON_ENABLED = new PropertyDescriptor.Builder()
            .name("dbrecord-lookup-clear-cache-on-enabled")
            .displayName("Clear Cache on Enabled")
            .description("Whether to clear the cache when this service is enabled. If the Cache Size is zero then this property is ignored. Clearing the cache when the "
                    + "service is enabled ensures that the service will first go to the database to get the most recent data.")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .required(true)
            .build();

    static final PropertyDescriptor CACHE_EXPIRATION = new PropertyDescriptor.Builder()
            .name("Cache Expiration")
            .description("Time interval to clear all cache entries. If the Cache Size is zero then this property is ignored.")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    protected List<PropertyDescriptor> properties;

    DBCPService dbcpService;

    volatile String lookupKeyColumn;

    protected List<Object> getCoordinates(final Map<String, Object> coordinates) {
        if (coordinates.isEmpty())
            return Collections.emptyList();

        List<Object> lookupCoordinates = new ArrayList<>();
        Object coordinate = coordinates.get(KEY);

        if (coordinate != null) {
            // Only one key is provided
            lookupCoordinates.add(coordinate);

        } else {
            // Multiple key's are provided in the form of key.1, key.2 .. key.x
            int i = 1;
            while (true) {
                coordinate = coordinates.get(String.format("%s.%d", KEY, i));
                i++;
                if (coordinate != null) {
                    lookupCoordinates.add(coordinate);
                } else {
                    break;
                }
            }
        }

        return lookupCoordinates;
    }

    protected abstract String sqlSelectList(Map<String, String> context);

    protected String buildSQLStatement(Map<String, String> context) {
        final String providedSQLStatement = getProperty(SQL_STATEMENT).evaluateAttributeExpressions(context).getValue();

        if (providedSQLStatement != null) {
            // Get the provided statement
            return getProperty(SQL_STATEMENT).evaluateAttributeExpressions(context).getValue();
        } else {
            // Or let's build one our self
            final String tableName = getProperty(TABLE_NAME).evaluateAttributeExpressions(context).getValue();

            final String selectQuery = "SELECT " + sqlSelectList(context) + " FROM " + tableName + " WHERE " + lookupKeyColumn + " = ?";
            return selectQuery;
        }
    }

    protected abstract List<PropertyDescriptor> getValueProperties();

    @Override
    protected void init(final ControllerServiceInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DBCP_SERVICE);
        properties.add(TABLE_NAME);
        properties.add(LOOKUP_KEY_COLUMN);
        properties.addAll(getValueProperties());
        properties.add(SQL_STATEMENT);
        properties.add(CACHE_SIZE);
        properties.add(CLEAR_CACHE_ON_ENABLED);
        properties.add(CACHE_EXPIRATION);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();
        String sqlStatement = validationContext.getProperty(SQL_STATEMENT).getValue();
        String tableName = validationContext.getProperty(TABLE_NAME).getValue();
        String keyColumn = validationContext.getProperty(LOOKUP_KEY_COLUMN).getValue();

        if (sqlStatement == null && tableName == null) {
            results.add(new ValidationResult.Builder()
                    .subject("Table Name or SQL Statement missing")
                    .valid(false)
                    .explanation("Please provide <Table Name> with <Lookup Key Column> for a simple select query. " +
                            "Or Provide a custom sql query under <SQL Statement>")
                    .build());
        }

        if (tableName != null && keyColumn == null) {
            results.add(new ValidationResult.Builder()
                    .subject("Key Column not set for Table Name mode")
                    .valid(false)
                    .explanation("If your are using the Table Name mode, Key Column must be set")
                    .build());
        }

        return results;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

}
