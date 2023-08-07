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
package org.apache.nifi.processors.azure.storage;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.core.http.HttpAuthorization;
import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.ManagedIdentityCredential;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.options.BlobUploadFromUrlOptions;
import com.azure.storage.blob.options.BlockBlobCommitBlockListOptions;
import com.azure.storage.blob.options.BlockBlobStageBlockFromUrlOptions;
import com.azure.storage.blob.sas.BlobContainerSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.AbstractAzureBlobProcessor_v12;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.services.azure.storage.AzureStorageConflictResolutionStrategy;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsDetails_v12;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsService_v12;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsType;
import reactor.core.publisher.Mono;

import java.text.DecimalFormat;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.azure.storage.blob.specialized.BlockBlobClient.MAX_STAGE_BLOCK_BYTES_LONG;
import static com.azure.storage.blob.specialized.BlockBlobClient.MAX_UPLOAD_BLOB_BYTES_LONG;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_BLOBNAME;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_BLOBTYPE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_CONTAINER;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_ERROR_CODE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_ETAG;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_IGNORED;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_LANG;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_LENGTH;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_MIME_TYPE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_PRIMARY_URI;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_TIMESTAMP;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_BLOBNAME;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_BLOBTYPE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_CONTAINER;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_ERROR_CODE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_ETAG;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_IGNORED;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_LANG;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_LENGTH;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_MIME_TYPE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_PRIMARY_URI;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_TIMESTAMP;

@Tags({"azure", "microsoft", "cloud", "storage", "blob"})
@SeeAlso({ListAzureBlobStorage_v12.class, FetchAzureBlobStorage_v12.class, DeleteAzureBlobStorage_v12.class, PutAzureBlobStorage_v12.class})
@CapabilityDescription("Copies a blob in Azure Blob Storage from one account/container to another. The processor uses Azure Blob Storage client library v12.")
@InputRequirement(Requirement.INPUT_REQUIRED)
@WritesAttributes({@WritesAttribute(attribute = ATTR_NAME_CONTAINER, description = ATTR_DESCRIPTION_CONTAINER),
        @WritesAttribute(attribute = ATTR_NAME_BLOBNAME, description = ATTR_DESCRIPTION_BLOBNAME),
        @WritesAttribute(attribute = ATTR_NAME_PRIMARY_URI, description = ATTR_DESCRIPTION_PRIMARY_URI),
        @WritesAttribute(attribute = ATTR_NAME_ETAG, description = ATTR_DESCRIPTION_ETAG),
        @WritesAttribute(attribute = ATTR_NAME_BLOBTYPE, description = ATTR_DESCRIPTION_BLOBTYPE),
        @WritesAttribute(attribute = ATTR_NAME_MIME_TYPE, description = ATTR_DESCRIPTION_MIME_TYPE),
        @WritesAttribute(attribute = ATTR_NAME_LANG, description = ATTR_DESCRIPTION_LANG),
        @WritesAttribute(attribute = ATTR_NAME_TIMESTAMP, description = ATTR_DESCRIPTION_TIMESTAMP),
        @WritesAttribute(attribute = ATTR_NAME_LENGTH, description = ATTR_DESCRIPTION_LENGTH),
        @WritesAttribute(attribute = ATTR_NAME_ERROR_CODE, description = ATTR_DESCRIPTION_ERROR_CODE),
        @WritesAttribute(attribute = ATTR_NAME_IGNORED, description = ATTR_DESCRIPTION_IGNORED)})
public class CopyAzureBlobStorage_v12 extends AbstractAzureBlobProcessor_v12 {
    private final static int GENERATE_SAS_EXPIRY_HOURS = 24;

    public static final PropertyDescriptor SOURCE_STORAGE_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .name("source-storage-credentials-service")
            .displayName("Source Storage Credentials")
            .description("Controller Service used to obtain Azure Blob Storage Credentials to read blob data ")
            .identifiesControllerService(AzureStorageCredentialsService_v12.class)
            .required(true)
            .build();

    public static final PropertyDescriptor SOURCE_CONTAINER = new PropertyDescriptor.Builder()
            .name("source-container-name")
            .displayName("Source Container Name")
            .description("Name of the Azure storage container to copy from.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor SOURCE_BLOB_NAME = new PropertyDescriptor.Builder()
            .name("source-blob-name")
            .displayName("Source Blob Name")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .description("The full name of the source blob")
            .build();

    public static final PropertyDescriptor DESTINATION_STORAGE_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(STORAGE_CREDENTIALS_SERVICE)
            .displayName("Destination Storage Credentials")
            .build();

    public static final PropertyDescriptor DESTINATION_CONTAINER = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.CONTAINER)
            .displayName("Destination Container Name")
            .description("Name of the Azure storage container to copy into; defaults to source container name.")
            .required(false)
            .build();

    public static final PropertyDescriptor DESTINATION_BLOB_NAME = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(BLOB_NAME)
            .displayName("Destination Blob Name")
            .description("The full name of the destination blob; defaults to source blob name.")
            .required(false)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            SOURCE_STORAGE_CREDENTIALS_SERVICE,
            SOURCE_CONTAINER,
            SOURCE_BLOB_NAME,
            DESTINATION_STORAGE_CREDENTIALS_SERVICE,
            DESTINATION_BLOB_NAME,
            DESTINATION_CONTAINER,
            AzureStorageUtils.CONFLICT_RESOLUTION,
            AzureStorageUtils.CREATE_CONTAINER,
            AzureStorageUtils.PROXY_CONFIGURATION_SERVICE
    ));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        super.onScheduled(context);
    }

    @OnStopped
    public void onStopped() {
        super.onStopped();
    }

    private static AzureStorageCredentialsService_v12 getCopyFromCredentialsService(ProcessContext context) {
        return context.getProperty(SOURCE_STORAGE_CREDENTIALS_SERVICE).asControllerService(AzureStorageCredentialsService_v12.class);
    }

    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String sourceContainerName = context.getProperty(SOURCE_CONTAINER).evaluateAttributeExpressions(flowFile).getValue();
        final String sourceBlobName = context.getProperty(SOURCE_BLOB_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String destinationContainerName = Optional.ofNullable(
                context.getProperty(DESTINATION_CONTAINER).evaluateAttributeExpressions(flowFile).getValue()
        ).orElse(sourceContainerName);
        final String destinationBlobName = Optional.ofNullable(
                context.getProperty(DESTINATION_BLOB_NAME).evaluateAttributeExpressions(flowFile).getValue()
        ).orElse(sourceBlobName);

        final boolean createContainer = context.getProperty(AzureStorageUtils.CREATE_CONTAINER).asBoolean();
        final AzureStorageConflictResolutionStrategy conflictResolution = AzureStorageConflictResolutionStrategy.valueOf(context.getProperty(AzureStorageUtils.CONFLICT_RESOLUTION).getValue());

        long startNanos = System.nanoTime();
        try {
            BlobServiceClient storageClient = getStorageClient(context, DESTINATION_STORAGE_CREDENTIALS_SERVICE, flowFile);
            BlobContainerClient containerClient = storageClient.getBlobContainerClient(destinationContainerName);
            if (createContainer && !containerClient.exists()) {
                containerClient.create();
            }

            BlobClient blobClient = containerClient.getBlobClient(destinationBlobName);
            Map<String, String> attributes = new HashMap<>();
            applyStandardBlobAttributes(attributes, blobClient);
            final boolean ignore = conflictResolution == AzureStorageConflictResolutionStrategy.IGNORE_RESOLUTION;

            final BlobRequestConditions destinationRequestConditions = new BlobRequestConditions();

            try {
                if (conflictResolution != AzureStorageConflictResolutionStrategy.REPLACE_RESOLUTION) {
                    destinationRequestConditions.setIfNoneMatch("*");
                }

                final AzureStorageCredentialsService_v12 fromCredentialsService = getCopyFromCredentialsService(context);
                BlobServiceClient fromStorageClient = getStorageClient(context, SOURCE_STORAGE_CREDENTIALS_SERVICE, flowFile);
                BlobContainerClient fromContainerClient = fromStorageClient.getBlobContainerClient(sourceContainerName);
                BlobClient fromBlobClient = fromContainerClient.getBlobClient(sourceBlobName);

                AzureStorageCredentialsDetails_v12 credentialsDetails = fromCredentialsService.getCredentialsDetails(flowFile.getAttributes());
                String sourceUrl = fromBlobClient.getBlobUrl();
                final BlobProperties fromBlobProperties = fromBlobClient.getProperties();
                final long blobSize = fromBlobProperties.getBlobSize();

                final BlobRequestConditions sourceRequestConditions = new BlobRequestConditions();
                sourceRequestConditions.setIfMatch(fromBlobProperties.getETag());

                HttpAuthorization httpAuthorization;
                final String sasToken = (credentialsDetails.getCredentialsType() == AzureStorageCredentialsType.ACCOUNT_KEY)
                        ? generateSas(fromContainerClient)
                        : credentialsDetails.getSasToken();
                if (sasToken != null) {
                    sourceUrl += "?" + sasToken;
                    httpAuthorization = null;
                } else {
                    httpAuthorization = getHttpAuthorization(credentialsDetails);
                }

                copy(blobClient, httpAuthorization, sourceUrl, blobSize, sourceRequestConditions, destinationRequestConditions);
                applyBlobMetadata(attributes, blobClient);

                if (ignore) {
                    attributes.put(ATTR_NAME_IGNORED, "false");
                }
            } catch (BlobStorageException e) {
                final BlobErrorCode errorCode = e.getErrorCode();
                flowFile = session.putAttribute(flowFile, ATTR_NAME_ERROR_CODE, e.getErrorCode().toString());

                if (errorCode == BlobErrorCode.BLOB_ALREADY_EXISTS && ignore) {
                    getLogger().info("Blob already exists: remote blob not modified. Transferring {} to success", flowFile);
                    attributes.put(ATTR_NAME_IGNORED, "true");
                } else {
                    throw e;
                }
            }

            flowFile = session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, REL_SUCCESS);

            long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            String transitUri = attributes.get(ATTR_NAME_PRIMARY_URI);
            session.getProvenanceReporter().send(flowFile, transitUri, transferMillis);
        } catch (Exception e) {
            getLogger().error("Failed to create blob on Azure Blob Storage", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private static String generateSas(final BlobContainerClient fromContainerClient) {
        BlobContainerSasPermission permissions = new BlobContainerSasPermission().setCreatePermission(true).setWritePermission(true).setAddPermission(true).setReadPermission(true);
        OffsetDateTime utc = OffsetDateTime.now(ZoneOffset.UTC);
        OffsetDateTime expiryTime = utc.plus(CopyAzureBlobStorage_v12.GENERATE_SAS_EXPIRY_HOURS, ChronoUnit.HOURS);
        BlobServiceSasSignatureValues signatureValues = new BlobServiceSasSignatureValues(expiryTime, permissions);
        return fromContainerClient.generateSas(signatureValues);
    }

    private void copy(final BlobClient blobClient,
                      final HttpAuthorization httpAuthorization,
                      final String sourceUrl,
                      long blobSize,
                      final BlobRequestConditions sourceRequestConditions,
                      final BlobRequestConditions destinationRequestConditions) {
        BlockBlobClient blockBlobClient = blobClient.getBlockBlobClient();

        // If the blob size is below the limit, we use the one-shot upload endpoint.
        if (blobSize < MAX_UPLOAD_BLOB_BYTES_LONG) {
            final BlobUploadFromUrlOptions options = new BlobUploadFromUrlOptions(sourceUrl);
            if (httpAuthorization != null) {
                options.setSourceAuthorization(httpAuthorization);
            }
            options.setSourceRequestConditions(sourceRequestConditions);
            options.setDestinationRequestConditions(destinationRequestConditions);
            blockBlobClient.uploadFromUrlWithResponse(options, null, Context.NONE);
            return;
        }

        final DecimalFormat df = new DecimalFormat("0000000");
        long offset = 0;
        int blockId = 1;
        final List<String> blockIds = new ArrayList<>();

        // Upload each block sequentially until we've uploaded the entire blob.
        while (true) {
            long count = Math.min(blobSize - offset, MAX_STAGE_BLOCK_BYTES_LONG);
            if (count == 0) break;

            final String base64BlockId = Base64.getEncoder().encodeToString(df.format(blockId).getBytes());
            BlockBlobStageBlockFromUrlOptions blockBlobStageBlockFromUrlOptions = new BlockBlobStageBlockFromUrlOptions(base64BlockId, sourceUrl);
            blockBlobStageBlockFromUrlOptions.setSourceRange(new BlobRange(offset, count));

            if (httpAuthorization != null) {
                blockBlobStageBlockFromUrlOptions.setSourceAuthorization(httpAuthorization);
            }
            blockBlobStageBlockFromUrlOptions.setSourceRequestConditions(sourceRequestConditions);
            final int statusCode = blockBlobClient.stageBlockFromUrlWithResponse(blockBlobStageBlockFromUrlOptions, null, Context.NONE).getStatusCode();
            if (statusCode != 201) {
                throw new ProcessException(String.format("Failed staging one or more blocks (status: %d)", statusCode));
            }
            blockIds.add(base64BlockId);
            offset += count;
            blockId++;
        }

        final BlockBlobCommitBlockListOptions options = new BlockBlobCommitBlockListOptions(blockIds);
        options.setRequestConditions(destinationRequestConditions);
        final Response<BlockBlobItem> response = blockBlobClient.commitBlockListWithResponse(options, null, Context.NONE);
        final int statusCode = response.getStatusCode();
        if (statusCode != 201) {
            throw new ProcessException(String.format("Failed committing block list (status: %d)", statusCode));
        }
    }

    private static HttpAuthorization getHttpAuthorization(final AzureStorageCredentialsDetails_v12 credentialsDetails) {
        switch (credentialsDetails.getCredentialsType()) {
            case ACCESS_TOKEN -> {
                TokenCredential credential = tokenRequestContext -> Mono.just(credentialsDetails.getAccessToken());
                return getHttpAuthorizationFromTokenCredential(credential);
            }
            case MANAGED_IDENTITY -> {
                final ManagedIdentityCredential credential = new ManagedIdentityCredentialBuilder()
                        .clientId(credentialsDetails.getManagedIdentityClientId())
                        .build();
                return getHttpAuthorizationFromTokenCredential(credential);
            }
            case SERVICE_PRINCIPAL -> {
                final ClientSecretCredential credential = new ClientSecretCredentialBuilder().clientId(
                                credentialsDetails.getServicePrincipalClientId()
                        ).clientSecret(credentialsDetails.getServicePrincipalClientSecret())
                        .tenantId(credentialsDetails.getServicePrincipalTenantId()).build();
                return getHttpAuthorizationFromTokenCredential(credential);
            }
        }
        return null;
    }

    private static HttpAuthorization getHttpAuthorizationFromTokenCredential(final TokenCredential credential) {
        final TokenRequestContext tokenRequestContext = new TokenRequestContext();
        tokenRequestContext.setScopes(Collections.singletonList("https://storage.azure.com/.default"));
        final AccessToken accessToken = credential.getToken(tokenRequestContext).block();
        final String authorization = accessToken.getToken();
        return new HttpAuthorization("Bearer", authorization);
    }
}
