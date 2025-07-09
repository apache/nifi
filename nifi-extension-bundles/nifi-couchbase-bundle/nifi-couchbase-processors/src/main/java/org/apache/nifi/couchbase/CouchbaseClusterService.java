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
package org.apache.nifi.couchbase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.Cluster;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.DeprecationNotice;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import com.couchbase.client.java.Bucket;

/**
 * Provides a centralized Couchbase connection and bucket passwords management.
 */
@CapabilityDescription("Provides a centralized Couchbase connection management.")
@Tags({ "nosql", "couchbase", "database", "connection" })
@DeprecationNotice(reason = "This component is deprecated and will be removed in NiFi 2.x.")
public class CouchbaseClusterService extends AbstractControllerService implements CouchbaseClusterControllerService {

    public static final PropertyDescriptor CONNECTION_STRING = new PropertyDescriptor
            .Builder()
            .name("Connection String")
            .description("The hostnames or ip addresses of the bootstraping nodes and optional parameters."
                    + " Syntax) couchbase://node1,node2,nodeN?param1=value1&param2=value2&paramN=valueN")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor USER_NAME = new PropertyDescriptor
            .Builder()
            .name("user-name")
            .displayName("User Name")
            .description("The user name to authenticate NiFi as a Couchbase client." +
                    " This configuration can be used against Couchbase Server 5.0 or later" +
                    " supporting Roll-Based Access Control.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor USER_PASSWORD = new PropertyDescriptor
            .Builder()
            .name("user-password")
            .displayName("User Password")
            .description("The user password to authenticate NiFi as a Couchbase client." +
                    " This configuration can be used against Couchbase Server 5.0 or later" +
                    " supporting Roll-Based Access Control.")
            .required(false)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(CONNECTION_STRING);
        props.add(USER_NAME);
        props.add(USER_PASSWORD);

        properties = Collections.unmodifiableList(props);
    }

    private volatile Cluster cluster;
    private String connectionString;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(
            String propertyDescriptorName) {
        return null;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final Collection<ValidationResult> results = new ArrayList<>();

        final boolean isUserNameSet = context.getProperty(USER_NAME).isSet();
        final boolean isUserPasswordSet = context.getProperty(USER_PASSWORD).isSet();
        if ((isUserNameSet && !isUserPasswordSet) || (!isUserNameSet && isUserPasswordSet)) {
            results.add(new ValidationResult.Builder()
                    .subject("User Name and Password")
                    .explanation("Both User Name and Password are required to use.")
                    .build());
        }

        return results;
    }

    /**
     * Establish a connection to a Couchbase cluster.
     * @param context the configuration context
     * @throws InitializationException if unable to connect a Couchbase cluster
     */
    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {

        connectionString = context.getProperty(CONNECTION_STRING).evaluateAttributeExpressions().getValue();
        final String userName = context.getProperty(USER_NAME).evaluateAttributeExpressions().getValue();
        final String userPassword = context.getProperty(USER_PASSWORD).evaluateAttributeExpressions().getValue();

        try {
            cluster = Cluster.connect(connectionString, userName, userPassword);
        } catch (CouchbaseException e) {
            throw new InitializationException(e);
        }
    }

    @Override
    public Bucket openBucket(String bucketName) {
        return cluster.bucket(bucketName);
    }

    @Override
    public com.couchbase.client.java.Collection openCollection(String bucketName, String scopeName, String collectionName) {
        return cluster.bucket(bucketName).scope(scopeName).collection(collectionName);
    }

    @Override
    public String connectionString() {
        return connectionString;
    }

    /**
     * Disconnect from the Couchbase cluster.
     */
    @OnDisabled
    public void shutdown() {
        if (cluster != null) {
            cluster.disconnect();
            cluster = null;
        }
    }

}
