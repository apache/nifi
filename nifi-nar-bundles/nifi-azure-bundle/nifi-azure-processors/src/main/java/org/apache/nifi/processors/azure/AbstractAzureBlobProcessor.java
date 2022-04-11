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

import com.microsoft.azure.keyvault.cryptography.SymmetricKey;
import com.microsoft.azure.storage.blob.BlobEncryptionPolicy;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.storage.utils.AzureBlobClientSideEncryptionMethod;
import org.apache.nifi.processors.azure.storage.utils.AzureBlobClientSideEncryptionUtils;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AbstractAzureBlobProcessor extends AbstractProcessor {
    private static final Pattern STRIP_LEADING_PATH_CHARS_PATTERN = Pattern.compile("^[./]+");

    public static final PropertyDescriptor BLOB = new PropertyDescriptor.Builder()
            .name("blob")
            .displayName("Blob Name")
            .description("The filename of the blob")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .defaultValue("${path}/${filename}")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All successfully processed FlowFiles are routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Unsuccessful operations will be transferred to the failure relationship.")
            .build();


    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        AbstractAzureBlobProcessor.REL_SUCCESS,
        AbstractAzureBlobProcessor.REL_FAILURE)));


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

    protected BlobRequestOptions createBlobRequestOptions(ProcessContext context) throws DecoderException {
        final String cseKeyTypeValue = context.getProperty(AzureBlobClientSideEncryptionUtils.CSE_KEY_TYPE).getValue();
        final AzureBlobClientSideEncryptionMethod cseKeyType = AzureBlobClientSideEncryptionMethod.valueOf(cseKeyTypeValue);

        final String cseKeyId = context.getProperty(AzureBlobClientSideEncryptionUtils.CSE_KEY_ID).getValue();

        final String cseSymmetricKeyHex = context.getProperty(AzureBlobClientSideEncryptionUtils.CSE_SYMMETRIC_KEY_HEX).getValue();

        BlobRequestOptions blobRequestOptions = new BlobRequestOptions();

        if (cseKeyType == AzureBlobClientSideEncryptionMethod.SYMMETRIC) {
            byte[] keyBytes = Hex.decodeHex(cseSymmetricKeyHex.toCharArray());
            SymmetricKey key = new SymmetricKey(cseKeyId, keyBytes);
            BlobEncryptionPolicy policy = new BlobEncryptionPolicy(key, null);
            blobRequestOptions.setEncryptionPolicy(policy);
        }

        return blobRequestOptions;
    }

    public String getBlobName(final ProcessContext context, final FlowFile flowFile) {
        final String blobName = context.getProperty(BLOB).evaluateAttributeExpressions(flowFile).getValue();
        if (blobName == null) {
            return null;
        }

        final Matcher matcher = STRIP_LEADING_PATH_CHARS_PATTERN.matcher(blobName);
        if (matcher.find()) {
            return matcher.replaceAll("");
        }

        return blobName;
    }
}
