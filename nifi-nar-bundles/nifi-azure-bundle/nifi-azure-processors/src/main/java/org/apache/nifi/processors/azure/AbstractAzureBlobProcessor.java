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
package org.apache.nifi.processors.azure;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class AbstractAzureBlobProcessor extends AbstractProcessor {

    public static final PropertyDescriptor BLOB = new PropertyDescriptor.Builder()
            .name("blob")
            .displayName("Blob")
            .description("The filename of the blob")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .defaultValue("${azure.blobname}")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All successfully processed FlowFiles are routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Unsuccessful operations will be transferred to the failure relationship.")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections
            .unmodifiableList(Arrays.asList(
                    AzureStorageUtils.CONTAINER,
                    AzureStorageUtils.PROP_SAS_TOKEN,
                    AzureStorageUtils.ACCOUNT_NAME,
                    AzureStorageUtils.ACCOUNT_KEY,
                    BLOB,
                    AzureStorageUtils.PROXY_CONFIGURATION_SERVICE));

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(
                    AbstractAzureBlobProcessor.REL_SUCCESS,
                    AbstractAzureBlobProcessor.REL_FAILURE)));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Collection<ValidationResult> results = AzureStorageUtils.validateCredentialProperties(validationContext);
        AzureStorageUtils.validateProxySpec(validationContext, results);
        return results;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }
}
