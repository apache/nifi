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
import org.apache.commons.lang3.StringUtils;

import static java.util.Arrays.asList;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class AbstractAzureDataLakeStorageProcessor extends AbstractProcessor {

    public static final PropertyDescriptor ACCOUNT_KEY = new PropertyDescriptor.Builder()
        .name("storage-account-key").displayName("Storage Account Key")
        .description("The storage account key. This is an admin-like password providing access to every container in this account. It is recommended " +
            "one uses Shared Access Signature (SAS) token instead for fine-grained control with policies. " +
            "There are certain risks in allowing the account key to be stored as a flowfile " +
            "attribute. While it does provide for a more flexible flow by allowing the account key to " +
            "be fetched dynamically from a flow file attribute, care must be taken to restrict access to " +
            "the event provenance data (e.g. by strictly controlling the policies governing provenance for this Processor). " +
            "In addition, the provenance repositories may be put on encrypted disk partitions.")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(false)
        .sensitive(true).build();

    public static final PropertyDescriptor ACCOUNT_NAME = new PropertyDescriptor.Builder()
        .name("storage-account-name").displayName("Storage Account Name")
        .description("The storage account name.  There are certain risks in allowing the account name to be stored as a flowfile " +
            "attribute. While it does provide for a more flexible flow by allowing the account name to " +
            "be fetched dynamically from a flowfile attribute, care must be taken to restrict access to " +
            "the event provenance data (e.g. by strictly controlling the policies governing provenance for this Processor). " +
            "In addition, the provenance repositories may be put on encrypted disk partitions." +
            " Instead of defining the Storage Account Name, Storage Account Key and SAS Token properties directly on the processor, " +
            "the preferred way is to configure them through a controller service")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .sensitive(true).build();

    public static final PropertyDescriptor FILESYSTEM = new PropertyDescriptor.Builder().name("Filesystem-name")
        .displayName("Filesystem Name")
        .description("Name of the Azure Storage File System, It is assumed to be already existing")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .build();

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder().name("Directory-name")
        .displayName("Directory Name")
        .description("Name of the Azure Storage Directory, It is assumed to be already existing")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .build();

    public static final PropertyDescriptor PROP_SAS_TOKEN = new PropertyDescriptor.Builder()
        .name("storage-sas-token").displayName("SAS Token")
        .description("Shared Access Signature token, including the leading '?'. Specify either SAS Token (recommended) or Account Key. " +
            "There are certain risks in allowing the SAS token to be stored as a flowfile " +
            "attribute. While it does provide for a more flexible flow by allowing the account name to " +
            "be fetched dynamically from a flowfile attribute, care must be taken to restrict access to " +
            "the event provenance data (e.g. by strictly controlling the policies governing provenance for this Processor). " +
            "In addition, the provenance repositories may be put on encrypted disk partitions.")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .sensitive(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor FILE = new PropertyDescriptor.Builder().name("file-name").displayName("File Name")
        .description("The filename")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .defaultValue("nifi.${uuid}")
        .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description(
            "Files that have been successfully written to Azure storage are transferred to this relationship")
        .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description(
            "Files that could not be written to Azure storage for some reason are transferred to this relationship")
        .build();

    private static final List <PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(
        Arrays.asList(AbstractAzureDataLakeStorageProcessor.ACCOUNT_KEY, AbstractAzureDataLakeStorageProcessor.ACCOUNT_NAME,
            AbstractAzureDataLakeStorageProcessor.FILESYSTEM, AbstractAzureDataLakeStorageProcessor.DIRECTORY,
            AbstractAzureDataLakeStorageProcessor.PROP_SAS_TOKEN, AbstractAzureDataLakeStorageProcessor.FILE));

    private static final Set <Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet <>(
        asList(
            AbstractAzureDataLakeStorageProcessor.REL_SUCCESS,
            AbstractAzureDataLakeStorageProcessor.REL_FAILURE)));

    @Override
    protected List <PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    public static Collection <ValidationResult> validateCredentialProperties(final ValidationContext validationContext) {
        final List <ValidationResult> results = new ArrayList<>();
        final String accountName = validationContext.getProperty(ACCOUNT_NAME).getValue();
        final String accountKey = validationContext.getProperty(ACCOUNT_KEY).getValue();
        final String sasToken = validationContext.getProperty(PROP_SAS_TOKEN).getValue();

        if (StringUtils.isNotBlank(accountName)
        && ((StringUtils.isNotBlank(accountKey) && StringUtils.isNotBlank(sasToken)) || (StringUtils.isBlank(accountKey) && StringUtils.isBlank(sasToken)))) {
            results.add(new ValidationResult.Builder().subject("AzureStorageUtils Credentials").valid(false)
                .explanation("either " + ACCOUNT_NAME.getDisplayName() + " with " + ACCOUNT_KEY.getDisplayName() +
                    " or " + ACCOUNT_NAME.getDisplayName() + " with " + PROP_SAS_TOKEN.getDisplayName() +
                    " must be specified, not both")
                .build());
        }
        return results;
    }

    @Override
    protected Collection <ValidationResult> customValidate(final ValidationContext validationContext) {
        final Collection <ValidationResult> results = AbstractAzureDataLakeStorageProcessor.validateCredentialProperties(validationContext);
        return results;
    }

    @Override
    public Set <Relationship> getRelationships() {
        return RELATIONSHIPS;
    }
}
