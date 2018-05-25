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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Tags({ "dbcp", "jdbc", "database", "connection", "pooling", "store" })
@CapabilityDescription("Provides a DBCPService that can be used to dynamically select another DBCPService. This service " +
        "requires an attribute named 'database.name' to be passed in when asking for a connection, and will throw an exception " +
        "if the attribute is missing. The value of 'database.name' will be used to select the DBCPService that has been " +
        "registered with that name. This will allow multiple DBCPServices to be defined and registered, and then selected " +
        "dynamically at runtime by tagging flow files with the appropriate 'database.name' attribute.")
@DynamicProperty(name = "The ", value = "JDBC property value", expressionLanguageScope = ExpressionLanguageScope.NONE,
        description = "")
public class DBCPConnectionPoolLookup extends AbstractControllerService implements DBCPService {

    public static final String DATABASE_NAME_ATTRIBUTE = "database.name";

    private volatile Map<String,DBCPService> dbcpServiceMap;

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("The DBCPService to return when database.name = '" + propertyDescriptorName + "'")
                .identifiesControllerService(DBCPService.class)
                .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>();

        int numDefinedServices = 0;
        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.isDynamic()) {
                numDefinedServices++;
            }

            final String referencedId = context.getProperty(descriptor).getValue();
            if (this.getIdentifier().equals(referencedId)) {
                results.add(new ValidationResult.Builder()
                        .subject(descriptor.getDisplayName())
                        .explanation("the current service cannot be registered as a DBCPService to lookup")
                        .valid(false)
                        .build());
            }
        }

        if (numDefinedServices == 0) {
            results.add(new ValidationResult.Builder()
                    .subject(this.getClass().getSimpleName())
                    .explanation("at least one DBCPService must be defined via dynamic properties")
                    .valid(false)
                    .build());
        }

        return results;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final Map<String,DBCPService> serviceMap = new HashMap<>();

        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.isDynamic()) {
                final DBCPService dbcpService = context.getProperty(descriptor).asControllerService(DBCPService.class);
                serviceMap.put(descriptor.getName(), dbcpService);
            }
        }

        dbcpServiceMap = Collections.unmodifiableMap(serviceMap);
    }

    @OnDisabled
    public void onDisabled() {
        dbcpServiceMap = null;
    }

    @Override
    public Connection getConnection() throws ProcessException {
        throw new UnsupportedOperationException("Cannot lookup DBCPConnectionPool without attributes");
    }

    @Override
    public Connection getConnection(Map<String, String> attributes) throws ProcessException {
        if (!attributes.containsKey(DATABASE_NAME_ATTRIBUTE)) {
            throw new ProcessException("Attributes must contain an attribute name '" + DATABASE_NAME_ATTRIBUTE + "'");
        }

        final String databaseName = attributes.get(DATABASE_NAME_ATTRIBUTE);
        if (StringUtils.isBlank(databaseName)) {
            throw new ProcessException(DATABASE_NAME_ATTRIBUTE + " cannot be null or blank");
        }

        final DBCPService dbcpService = dbcpServiceMap.get(databaseName);
        if (dbcpService == null) {
            throw new ProcessException("No DBCPService was found for database.name '" + databaseName + "'");
        }

        return dbcpService.getConnection(attributes);
    }

}
