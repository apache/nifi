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
package org.apache.nifi.processors.aws.s3;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.AbstractAwsSyncProcessor;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.nifi.processors.aws.region.RegionUtil.CUSTOM_REGION;
import static org.apache.nifi.processors.aws.region.RegionUtil.REGION;
import static org.apache.nifi.processors.aws.region.RegionUtil.USE_CUSTOM_REGION;

public abstract class AbstractS3Processor extends AbstractAwsSyncProcessor<S3Client, S3ClientBuilderWrapper> {

    // Obsolete property names
    protected static final String OBSOLETE_WRITE_USER_LIST = "Write Permission User List";
    protected static final String OBSOLETE_OWNER = "Owner";

    private static final String OBSOLETE_SIGNER_OVERRIDE = "Signer Override";
    private static final String OBSOLETE_CUSTOM_SIGNER_CLASS_NAME_1 = "custom-signer-class-name";
    private static final String OBSOLETE_CUSTOM_SIGNER_CLASS_NAME_2 = "Custom Signer Class Name";
    private static final String OBSOLETE_CUSTOM_SIGNER_MODULE_LOCATION_1 = "custom-signer-module-location";
    private static final String OBSOLETE_CUSTOM_SIGNER_MODULE_LOCATION_2 = "Custom Signer Module Location";

    // Obsolete property value and attribute name
    private static final String OBSOLETE_ATTRIBUTE_DEFINED_REGION = "attribute-defined-region";
    private static final String OBSOLETE_S3_REGION_ATTRIBUTE = "s3.region";

    public static final PropertyDescriptor FULL_CONTROL_USER_LIST = new PropertyDescriptor.Builder()
            .name("FullControl User List")
            .description("A comma-separated list of Amazon User ID's or E-mail addresses that specifies who should have Full Control for an object")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("${s3.permissions.full.users}")
            .build();
    public static final PropertyDescriptor READ_USER_LIST = new PropertyDescriptor.Builder()
            .name("Read Permission User List")
            .description("A comma-separated list of Amazon User ID's or E-mail addresses that specifies who should have Read Access for an object")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("${s3.permissions.read.users}")
            .build();
    public static final PropertyDescriptor READ_ACL_LIST = new PropertyDescriptor.Builder()
            .name("Read ACL User List")
            .description("A comma-separated list of Amazon User ID's or E-mail addresses that specifies who should have permissions to read the Access Control List for an object")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("${s3.permissions.readacl.users}")
            .build();
    public static final PropertyDescriptor WRITE_ACL_LIST = new PropertyDescriptor.Builder()
            .name("Write ACL User List")
            .description("A comma-separated list of Amazon User ID's or E-mail addresses that specifies who should have permissions to change the Access Control List for an object")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("${s3.permissions.writeacl.users}")
            .build();
    public static final PropertyDescriptor CANNED_ACL = new PropertyDescriptor.Builder()
            .name("Canned ACL")
            .description("Amazon Canned ACL for an object, one of: BucketOwnerFullControl, BucketOwnerRead, AuthenticatedRead, PublicReadWrite, PublicRead, Private; " +
                "will be ignored if any other ACL/permission/owner property is specified")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("${s3.permissions.cannedacl}")
            .build();
    public static final PropertyDescriptor BUCKET_WITHOUT_DEFAULT_VALUE = new PropertyDescriptor.Builder()
            .name("Bucket")
            .description("The S3 Bucket to interact with")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor BUCKET_WITH_DEFAULT_VALUE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(BUCKET_WITHOUT_DEFAULT_VALUE)
            .defaultValue("${s3.bucket}")
            .build();
    public static final PropertyDescriptor KEY = new PropertyDescriptor.Builder()
            .name("Object Key")
            .description("The S3 Object Key to use. This is analogous to a filename for traditional file systems.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("${filename}")
            .build();
    public static final PropertyDescriptor VERSION_ID = new PropertyDescriptor.Builder()
            .name("Version")
            .description("The Version of the Object to download")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final PropertyDescriptor ENCRYPTION_SERVICE = new PropertyDescriptor.Builder()
            .name("Encryption Service")
            .description("Specifies the Encryption Service Controller used to configure requests. " +
                    "FetchS3Object: Only needs to be configured in case of Server-side Customer Key encryption.")
            .required(false)
            .identifiesControllerService(AmazonS3EncryptionService.class)
            .build();
    public static final PropertyDescriptor USE_CHUNKED_ENCODING = new PropertyDescriptor.Builder()
            .name("Use Chunked Encoding")
            .description("Enables / disables chunked encoding for upload requests. Set it to false only if your endpoint does not support chunked uploading.")
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();
    public static final PropertyDescriptor USE_PATH_STYLE_ACCESS = new PropertyDescriptor.Builder()
            .name("Use Path Style Access")
            .description("Path-style access can be enforced by setting this property to true. Set it to true if your endpoint does not support " +
                    "virtual-hosted-style requests, only path-style requests.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    // maps AWS SDK v1 CannedAccessControlList to v2 ObjectCannedACL
    private static final Map<String, ObjectCannedACL> CANNED_ACL_MAPPING = Map.of(
            "Private", ObjectCannedACL.PRIVATE,
            "PublicRead", ObjectCannedACL.PUBLIC_READ,
            "PublicReadWrite", ObjectCannedACL.PUBLIC_READ_WRITE,
            "AuthenticatedRead", ObjectCannedACL.AUTHENTICATED_READ,
            "BucketOwnerRead", ObjectCannedACL.BUCKET_OWNER_READ,
            "BucketOwnerFullControl", ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL,
            "AwsExecRead", ObjectCannedACL.AWS_EXEC_READ
    );

    static final String S3_ENCRYPTION_STRATEGY = "s3.encryptionStrategy";
    static final String S3_SSE_ALGORITHM = "s3.sseAlgorithm";

    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        super.migrateProperties(config);

        config.renameProperty("canned-acl", CANNED_ACL.getName());
        config.renameProperty("encryption-service", ENCRYPTION_SERVICE.getName());
        config.renameProperty("use-chunked-encoding", USE_CHUNKED_ENCODING.getName());
        config.renameProperty("use-path-style-access", USE_PATH_STYLE_ACCESS.getName());

        migrateAttributeDefinedRegion(config);
        migrateCannedAcl(config);

        config.removeProperty(OBSOLETE_SIGNER_OVERRIDE);
        config.removeProperty(OBSOLETE_CUSTOM_SIGNER_CLASS_NAME_1);
        config.removeProperty(OBSOLETE_CUSTOM_SIGNER_CLASS_NAME_2);
        config.removeProperty(OBSOLETE_CUSTOM_SIGNER_MODULE_LOCATION_1);
        config.removeProperty(OBSOLETE_CUSTOM_SIGNER_MODULE_LOCATION_2);
    }

    private void migrateAttributeDefinedRegion(final PropertyConfiguration config) {
        if (config.getPropertyValue(REGION).map(OBSOLETE_ATTRIBUTE_DEFINED_REGION::equals).orElse(false)) {
            // migrate Use 's3.region' Attribute option into Use Custom Region
            config.setProperty(REGION, USE_CUSTOM_REGION.getValue());
            config.setProperty(CUSTOM_REGION, String.format("${%s}",  OBSOLETE_S3_REGION_ATTRIBUTE));
        }
    }

    private void migrateCannedAcl(final PropertyConfiguration config) {
        if (config.getPropertyValue(CANNED_ACL).map("LogDeliveryWrite"::equals).orElse(false)) {
            // ObjectCannedACL in v2 does not include LogDeliveryWrite, it is a bucket level-permission that has no effect on object-level operations
            config.setProperty(CANNED_ACL, null);
        }
    }

    @Override
    protected S3ClientBuilderWrapper createClientBuilder(ProcessContext context) {
        final AmazonS3EncryptionService encryptionService = context.getProperty(ENCRYPTION_SERVICE).asControllerService(AmazonS3EncryptionService.class);

        final S3ClientBuilderWrapper clientBuilder = Optional.ofNullable(encryptionService)
                .map(AmazonS3EncryptionService::createEncryptionClientBuilder)
                .map(S3ClientBuilderWrapper::new)
                .orElse(new S3ClientBuilderWrapper(S3Client.builder()));

        final S3Configuration.Builder configurationBuilder = S3Configuration.builder();

        final Boolean useChunkedEncoding = context.getProperty(USE_CHUNKED_ENCODING).asBoolean();
        if (useChunkedEncoding == Boolean.FALSE) {
            configurationBuilder.chunkedEncodingEnabled(false);
        }

        final Boolean usePathStyleAccess = context.getProperty(USE_PATH_STYLE_ACCESS).asBoolean();
        final boolean endpointOverrideSet = StringUtils.isNotBlank(context.getProperty(ENDPOINT_OVERRIDE).evaluateAttributeExpressions().getValue());
        if (usePathStyleAccess == Boolean.TRUE || endpointOverrideSet) {
            configurationBuilder.pathStyleAccessEnabled(true);
        }

        clientBuilder.serviceConfiguration(configurationBuilder.build());

        return clientBuilder;
    }

    protected String getFullControlGranteeSpec(final PropertyContext context, final FlowFile flowFile) {
        return getGranteeSpec(context, flowFile, FULL_CONTROL_USER_LIST);
    }

    protected String getReadGranteeSpec(final PropertyContext context, final FlowFile flowFile) {
        return getGranteeSpec(context, flowFile, READ_USER_LIST);
    }

    protected String getReadACPGranteeSpec(final PropertyContext context, final FlowFile flowFile) {
        return getGranteeSpec(context, flowFile, READ_ACL_LIST);
    }

    protected String getWriteACPGranteeSpec(final PropertyContext context, final FlowFile flowFile) {
        return getGranteeSpec(context, flowFile, WRITE_ACL_LIST);
    }

    private String getGranteeSpec(final PropertyContext context, final FlowFile flowFile, final PropertyDescriptor propertyDescriptor) {
        final String value = context.getProperty(propertyDescriptor).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isBlank(value)) {
            return null;
        }

        return Arrays.stream(value.split(","))
                .filter(grantee -> !grantee.isBlank())
                .map(grantee -> {
                    if (grantee.contains("@")) {
                        // email address grantee
                        return String.format("emailAddress=%s", grantee);
                    } else {
                        // canonical user grantee
                        return String.format("id=%s", grantee);
                    }
                })
                .collect(Collectors.joining(","));
    }

    protected FlowFile extractExceptionDetails(final Exception e, final ProcessSession session, FlowFile flowFile) {
        flowFile = session.putAttribute(flowFile, "s3.exception", e.getClass().getName());
        if (e instanceof final AwsServiceException ase) {
            flowFile = putAttribute(session, flowFile, "s3.statusCode", ase.statusCode());
            final AwsErrorDetails errorDetails = ase.awsErrorDetails();
            if (errorDetails != null) {
                flowFile = putAttribute(session, flowFile, "s3.errorCode", errorDetails.errorCode());
                flowFile = putAttribute(session, flowFile, "s3.errorMessage", errorDetails.errorMessage());
                flowFile = putAttribute(session, flowFile, "s3.additionalDetails", errorDetails.sdkHttpResponse().headers());
            }
        }
        return flowFile;
    }

    private FlowFile putAttribute(final ProcessSession session, final FlowFile flowFile, final String key, final Object value) {
        return (value == null) ? flowFile : session.putAttribute(flowFile, key, value.toString());
    }

    /**
     * Create ObjectCannedACL if {@link #CANNED_ACL} property specified.
     *
     * @param context ProcessContext
     * @param flowFile FlowFile
     * @return ObjectCannedACL or null if not specified
     */
    protected final ObjectCannedACL createCannedACL(final ProcessContext context, final FlowFile flowFile) {
        if (getFullControlGranteeSpec(context, flowFile) != null
                || getReadGranteeSpec(context, flowFile) != null
                || getReadACPGranteeSpec(context, flowFile) != null
                || getWriteACPGranteeSpec(context, flowFile) != null) {
            return null;
        }

        ObjectCannedACL cannedAcl = null;

        final String cannedAclName = context.getProperty(CANNED_ACL).evaluateAttributeExpressions(flowFile).getValue();
        if (!StringUtils.isEmpty(cannedAclName)) {
            cannedAcl = CANNED_ACL_MAPPING.get(cannedAclName);
        }

        return cannedAcl;
    }

    protected void setEncryptionAttributes(final Map<String, String> attributes, final ServerSideEncryption serverSideEncryption, final String customerAlgorithm,
                                           final AmazonS3EncryptionService encryptionService) {
        if (serverSideEncryption == ServerSideEncryption.AES256) {
            attributes.put(S3_ENCRYPTION_STRATEGY, AmazonS3EncryptionService.STRATEGY_NAME_SSE_S3);
            attributes.put(S3_SSE_ALGORITHM, serverSideEncryption.toString());
        } else if (serverSideEncryption == ServerSideEncryption.AWS_KMS) {
            attributes.put(S3_ENCRYPTION_STRATEGY, AmazonS3EncryptionService.STRATEGY_NAME_SSE_KMS);
            attributes.put(S3_SSE_ALGORITHM, serverSideEncryption.toString());
        } else if (customerAlgorithm != null) {
            attributes.put(S3_ENCRYPTION_STRATEGY, AmazonS3EncryptionService.STRATEGY_NAME_SSE_C);
        } else if (encryptionService != null) {
            attributes.put(S3_ENCRYPTION_STRATEGY, encryptionService.getStrategyName());
        }
    }
}
