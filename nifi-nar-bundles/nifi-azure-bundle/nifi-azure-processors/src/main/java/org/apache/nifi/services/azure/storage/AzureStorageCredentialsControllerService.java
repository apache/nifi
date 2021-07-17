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
package org.apache.nifi.services.azure.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;

/**
 * Implementation of AbstractControllerService interface
 *
 * @see AbstractControllerService
 */
@Tags({ "azure", "microsoft", "cloud", "storage", "blob", "queue", "credentials" })
@CapabilityDescription("Defines credentials for Azure Storage processors. " +
        "Uses Account Name with Account Key or Account Name with SAS Token.")
public class AzureStorageCredentialsControllerService extends AbstractControllerService implements AzureStorageCredentialsService {

    public static final PropertyDescriptor ACCOUNT_NAME = new PropertyDescriptor.Builder()
            .name(AzureStorageUtils.ACCOUNT_NAME.getName())
            .displayName(AzureStorageUtils.ACCOUNT_NAME.getDisplayName())
            .description(AzureStorageUtils.ACCOUNT_NAME_BASE_DESCRIPTION + AzureStorageUtils.ACCOUNT_NAME_SECURITY_DESCRIPTION)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .sensitive(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections
            .unmodifiableList(Arrays.asList(
                    ACCOUNT_NAME,
                    AzureStorageUtils.ACCOUNT_KEY,
                    AzureStorageUtils.PROP_SAS_TOKEN,
                    AzureStorageUtils.ENDPOINT_SUFFIX));

    private ConfigurationContext context;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final String accountKey = validationContext.getProperty(AzureStorageUtils.ACCOUNT_KEY).getValue();
        final String sasToken = validationContext.getProperty(AzureStorageUtils.PROP_SAS_TOKEN).getValue();

        if (StringUtils.isBlank(accountKey) && StringUtils.isBlank(sasToken)) {
            results.add(new ValidationResult.Builder().subject("AzureStorageCredentialsControllerService")
                    .valid(false)
                    .explanation("either " + AzureStorageUtils.ACCOUNT_KEY.getDisplayName() + " or " + AzureStorageUtils.PROP_SAS_TOKEN.getDisplayName() + " is required")
                    .build());
        } else if (StringUtils.isNotBlank(accountKey) && StringUtils.isNotBlank(sasToken)) {
            results.add(new ValidationResult.Builder().subject("AzureStorageCredentialsControllerService")
                        .valid(false)
                        .explanation("cannot set both " + AzureStorageUtils.ACCOUNT_KEY.getDisplayName() + " and " + AzureStorageUtils.PROP_SAS_TOKEN.getDisplayName())
                        .build());
        }

        return results;
    }

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        this.context = context;
    }

    @Override
    public AzureStorageCredentialsDetails getStorageCredentialsDetails(Map<String, String> attributes) {
        return AzureStorageUtils.createStorageCredentialsDetails(context, attributes);
    }
}
