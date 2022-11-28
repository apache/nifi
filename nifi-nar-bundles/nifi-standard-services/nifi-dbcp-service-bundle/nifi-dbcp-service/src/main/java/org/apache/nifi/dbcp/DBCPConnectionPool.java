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

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.behavior.SupportsSensitiveDynamicProperties;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.kerberos.KerberosUserService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Implementation of for Database Connection Pooling Service. Apache DBCP is used for connection pooling functionality.
 *
 */
@SupportsSensitiveDynamicProperties
@Tags({ "dbcp", "jdbc", "database", "connection", "pooling", "store" })
@CapabilityDescription("Provides Database Connection Pooling Service. Connections can be asked from pool and returned after usage.")
@DynamicProperties({
        @DynamicProperty(name = "JDBC property name",
                value = "JDBC property value",
                expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY,
                description = "JDBC driver property name and value applied to JDBC connections."),
        @DynamicProperty(name = "SENSITIVE.JDBC property name",
                value = "JDBC property value",
                expressionLanguageScope = ExpressionLanguageScope.NONE,
                description = "JDBC driver property name prefixed with 'SENSITIVE.' handled as a sensitive property.")
})
@RequiresInstanceClassLoading
public class DBCPConnectionPool extends AbstractDBCPConnectionPool implements DBCPService, VerifiableControllerService {

    private static final List<PropertyDescriptor> PROPERTIES;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(DATABASE_URL);
        props.add(DB_DRIVERNAME);
        props.add(DB_DRIVER_LOCATION);
        props.add(KERBEROS_USER_SERVICE);
        props.add(KERBEROS_CREDENTIALS_SERVICE);
        props.add(KERBEROS_PRINCIPAL);
        props.add(KERBEROS_PASSWORD);
        props.add(DB_USER);
        props.add(DB_PASSWORD);
        props.add(MAX_WAIT_TIME);
        props.add(MAX_TOTAL_CONNECTIONS);
        props.add(VALIDATION_QUERY);
        props.add(MIN_IDLE);
        props.add(MAX_IDLE);
        props.add(MAX_CONN_LIFETIME);
        props.add(EVICTION_RUN_PERIOD);
        props.add(MIN_EVICTABLE_IDLE_TIME);
        props.add(SOFT_MIN_EVICTABLE_IDLE_TIME);

        PROPERTIES = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>();

        final boolean kerberosPrincipalProvided = !StringUtils.isBlank(context.getProperty(KERBEROS_PRINCIPAL).evaluateAttributeExpressions().getValue());
        final boolean kerberosPasswordProvided = !StringUtils.isBlank(context.getProperty(KERBEROS_PASSWORD).getValue());

        if (kerberosPrincipalProvided && !kerberosPasswordProvided) {
            results.add(new ValidationResult.Builder()
                    .subject(KERBEROS_PASSWORD.getDisplayName())
                    .valid(false)
                    .explanation("a password must be provided for the given principal")
                    .build());
        }

        if (kerberosPasswordProvided && !kerberosPrincipalProvided) {
            results.add(new ValidationResult.Builder()
                    .subject(KERBEROS_PRINCIPAL.getDisplayName())
                    .valid(false)
                    .explanation("a principal must be provided for the given password")
                    .build());
        }

        final KerberosCredentialsService kerberosCredentialsService = context.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);
        final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);

        if (kerberosCredentialsService != null && (kerberosPrincipalProvided || kerberosPasswordProvided)) {
            results.add(new ValidationResult.Builder()
                    .subject(KERBEROS_CREDENTIALS_SERVICE.getDisplayName())
                    .valid(false)
                    .explanation("kerberos principal/password and kerberos credential service cannot be configured at the same time")
                    .build());
        }

        if (kerberosUserService != null && (kerberosPrincipalProvided || kerberosPasswordProvided)) {
            results.add(new ValidationResult.Builder()
                    .subject(KERBEROS_USER_SERVICE.getDisplayName())
                    .valid(false)
                    .explanation("kerberos principal/password and kerberos user service cannot be configured at the same time")
                    .build());
        }

        if (kerberosUserService != null && kerberosCredentialsService != null) {
            results.add(new ValidationResult.Builder()
                    .subject(KERBEROS_USER_SERVICE.getDisplayName())
                    .valid(false)
                    .explanation("kerberos user service and kerberos credential service cannot be configured at the same time")
                    .build());
        }

        return results;
    }

    BasicDataSource getDataSource() {
        return dataSource;
    }
}
