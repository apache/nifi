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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.Signer;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Builder;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.EmailAddressGrantee;
import com.amazonaws.services.s3.model.Grantee;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.Permission;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;
import org.apache.nifi.processors.aws.signer.AwsCustomSignerUtil;
import org.apache.nifi.processors.aws.signer.AwsSignerType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.nifi.processors.aws.signer.AwsSignerType.AWS_S3_V2_SIGNER;
import static org.apache.nifi.processors.aws.signer.AwsSignerType.AWS_S3_V4_SIGNER;
import static org.apache.nifi.processors.aws.signer.AwsSignerType.CUSTOM_SIGNER;
import static org.apache.nifi.processors.aws.signer.AwsSignerType.DEFAULT_SIGNER;
import static org.apache.nifi.processors.aws.util.RegionUtilV1.ATTRIBUTE_DEFINED_REGION;
import static org.apache.nifi.processors.aws.util.RegionUtilV1.S3_REGION;
import static org.apache.nifi.processors.aws.util.RegionUtilV1.resolveS3Region;

public abstract class AbstractS3Processor extends AbstractAWSCredentialsProviderProcessor<AmazonS3Client> {

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
    public static final PropertyDescriptor WRITE_USER_LIST = new PropertyDescriptor.Builder()
            .name("Write Permission User List")
            .description("A comma-separated list of Amazon User ID's or E-mail addresses that specifies who should have Write Access for an object")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("${s3.permissions.write.users}")
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
            .name("canned-acl")
            .displayName("Canned ACL")
            .description("Amazon Canned ACL for an object, one of: BucketOwnerFullControl, BucketOwnerRead, LogDeliveryWrite, AuthenticatedRead, PublicReadWrite, PublicRead, Private; " +
                "will be ignored if any other ACL/permission/owner property is specified")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("${s3.permissions.cannedacl}")
            .build();
    public static final PropertyDescriptor OWNER = new PropertyDescriptor.Builder()
            .name("Owner")
            .description("The Amazon ID to use for the object's owner")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("${s3.owner}")
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
    public static final PropertyDescriptor SIGNER_OVERRIDE = new PropertyDescriptor.Builder()
            .name("Signer Override")
            .description("The AWS S3 library uses Signature Version 4 by default but this property allows you to specify the Version 2 signer to support older S3-compatible services" +
                    " or even to plug in your own custom signer implementation.")
            .required(false)
            .allowableValues(EnumSet.of(
                            DEFAULT_SIGNER,
                            AWS_S3_V4_SIGNER,
                            AWS_S3_V2_SIGNER,
                            CUSTOM_SIGNER))
            .defaultValue(DEFAULT_SIGNER.getValue())
            .build();

    public static final PropertyDescriptor S3_CUSTOM_SIGNER_CLASS_NAME = new PropertyDescriptor.Builder()
            .name("custom-signer-class-name")
            .displayName("Custom Signer Class Name")
            .description(String.format("Fully qualified class name of the custom signer class. The signer must implement %s interface.", Signer.class.getName()))
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(SIGNER_OVERRIDE, CUSTOM_SIGNER)
            .build();

    public static final PropertyDescriptor S3_CUSTOM_SIGNER_MODULE_LOCATION = new PropertyDescriptor.Builder()
            .name("custom-signer-module-location")
            .displayName("Custom Signer Module Location")
            .description("Comma-separated list of paths to files and/or directories which contain the custom signer's JAR file and its dependencies (if any).")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE, ResourceType.DIRECTORY)
            .dependsOn(SIGNER_OVERRIDE, CUSTOM_SIGNER)
            .dynamicallyModifiesClasspath(true)
            .build();

    public static final PropertyDescriptor ENCRYPTION_SERVICE = new PropertyDescriptor.Builder()
            .name("encryption-service")
            .displayName("Encryption Service")
            .description("Specifies the Encryption Service Controller used to configure requests. " +
                    "PutS3Object: For backward compatibility, this value is ignored when 'Server Side Encryption' is set. " +
                    "FetchS3Object: Only needs to be configured in case of Server-side Customer Key, Client-side KMS and Client-side Customer Key encryptions.")
            .required(false)
            .identifiesControllerService(AmazonS3EncryptionService.class)
            .build();
    public static final PropertyDescriptor USE_CHUNKED_ENCODING = new PropertyDescriptor.Builder()
            .name("use-chunked-encoding")
            .displayName("Use Chunked Encoding")
            .description("Enables / disables chunked encoding for upload requests. Set it to false only if your endpoint does not support chunked uploading.")
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();
    public static final PropertyDescriptor USE_PATH_STYLE_ACCESS = new PropertyDescriptor.Builder()
            .name("use-path-style-access")
            .displayName("Use Path Style Access")
            .description("Path-style access can be enforced by setting this property to true. Set it to true if your endpoint does not support " +
                    "virtual-hosted-style requests, only path-style requests.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    /**
     * Create client using credentials provider. This is the preferred way for creating clients
     */
    @Override
    protected AmazonS3Client createClient(final ProcessContext context, final AWSCredentialsProvider credentialsProvider, final Region region,
                                          final ClientConfiguration config, final AwsClientBuilder.EndpointConfiguration endpointConfiguration) {
        getLogger().info("Creating client with credentials provider");
        initializeSignerOverride(context, config);
        AmazonS3EncryptionService encryptionService = context.getProperty(ENCRYPTION_SERVICE).asControllerService(AmazonS3EncryptionService.class);

        final Consumer<AmazonS3Builder<?, ?>> clientBuilder = builder -> {
            if (endpointConfiguration == null) {
                builder.withRegion(region.getName());
            } else {
                builder.withEndpointConfiguration(endpointConfiguration);
            }
            builder.withClientConfiguration(config);
            builder.withCredentials(credentialsProvider);

            final Boolean useChunkedEncoding = context.getProperty(USE_CHUNKED_ENCODING).asBoolean();
            if (useChunkedEncoding == Boolean.FALSE) {
                builder.disableChunkedEncoding();
            }

            final Boolean usePathStyleAccess = context.getProperty(USE_PATH_STYLE_ACCESS).asBoolean();
            final boolean endpointOverrideSet = !StringUtils.trimToEmpty(context.getProperty(ENDPOINT_OVERRIDE).evaluateAttributeExpressions().getValue()).isEmpty();
            if (usePathStyleAccess == Boolean.TRUE || endpointOverrideSet) {
                builder.withPathStyleAccessEnabled(true);
            }
        };

        AmazonS3 s3Client = null;
        if (encryptionService != null) {
            s3Client = encryptionService.createEncryptionClient(clientBuilder);
        }
        if (s3Client == null) {
            final AmazonS3ClientBuilder builder = AmazonS3Client.builder();
            clientBuilder.accept(builder);
            s3Client = builder.build();
        }

        return (AmazonS3Client) s3Client;
    }


    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog verificationLogger, final Map<String, String> attributes) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        try {
            createClient(context, attributes);
            results.add(new ConfigVerificationResult.Builder()
                    .outcome(Outcome.SUCCESSFUL)
                    .verificationStepName("Create S3 Client")
                    .explanation("Successfully created S3 Client")
                    .build());
        } catch (final Exception e) {
            verificationLogger.error("Failed to create S3 Client", e);
            results.add(new ConfigVerificationResult.Builder()
                    .outcome(Outcome.FAILED)
                    .verificationStepName("Create S3 Client")
                    .explanation("Failed to crete S3 Client: " + e.getMessage())
                    .build());
        }

        return results;
    }

    /**
     * Creates and configures the client from the context and FlowFile attributes or returns an existing client from cache
     * @param context the process context
     * @param attributes FlowFile attributes
     * @return The created S3 client
     */
    protected AmazonS3Client getS3Client(final ProcessContext context, final Map<String, String> attributes) {
        final Region region = resolveS3Region(context, attributes);
        return getClient(context, region);
    }

    /**
     * Creates the client from the context and FlowFile attributes
     * @param context the process context
     * @param attributes FlowFile attributes
     * @return The newly created S3 client
     */
    protected AmazonS3Client createClient(final ProcessContext context, final Map<String, String> attributes) {
        final Region region = resolveS3Region(context, attributes);
        return createClient(context, region);
    }

    @Override
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        if (!isAttributeDefinedRegion(context)) {
            getClient(context);
        }
    }


    private void initializeSignerOverride(final ProcessContext context, final ClientConfiguration config) {
        final String signer = context.getProperty(SIGNER_OVERRIDE).getValue();
        final AwsSignerType signerType = AwsSignerType.forValue(signer);

        if (signerType == CUSTOM_SIGNER) {
            final String signerClassName = context.getProperty(S3_CUSTOM_SIGNER_CLASS_NAME).evaluateAttributeExpressions().getValue();

            config.setSignerOverride(AwsCustomSignerUtil.registerCustomSigner(signerClassName));
        } else if (signerType != DEFAULT_SIGNER) {
            config.setSignerOverride(signer);
        }
    }


    protected Grantee createGrantee(final String value) {
        if (StringUtils.isEmpty(value)) {
            return null;
        }

        if (value.contains("@")) {
            return new EmailAddressGrantee(value);
        } else {
            return new CanonicalGrantee(value);
        }
    }

    protected final List<Grantee> createGrantees(final String value) {
        if (StringUtils.isEmpty(value)) {
            return Collections.emptyList();
        }

        final List<Grantee> grantees = new ArrayList<>();
        final String[] vals = value.split(",");
        for (final String val : vals) {
            final String identifier = val.trim();
            final Grantee grantee = createGrantee(identifier);
            if (grantee != null) {
                grantees.add(grantee);
            }
        }
        return grantees;
    }

    /**
     * Create AccessControlList if appropriate properties are configured.
     *
     * @param context ProcessContext
     * @param flowFile FlowFile
     * @return AccessControlList or null if no ACL properties were specified
     */
    protected final AccessControlList createACL(final ProcessContext context, final FlowFile flowFile) {
        // lazy-initialize ACL, as it should not be used if no properties were specified
        AccessControlList acl = null;

        final String ownerId = context.getProperty(OWNER).evaluateAttributeExpressions(flowFile).getValue();
        if (!StringUtils.isEmpty(ownerId)) {
            final Owner owner = new Owner();
            owner.setId(ownerId);
            acl = new AccessControlList();
            acl.setOwner(owner);
        }

        for (final Grantee grantee : createGrantees(context.getProperty(FULL_CONTROL_USER_LIST).evaluateAttributeExpressions(flowFile).getValue())) {
            if (acl == null) {
                acl = new AccessControlList();
            }
            acl.grantPermission(grantee, Permission.FullControl);
        }

        for (final Grantee grantee : createGrantees(context.getProperty(READ_USER_LIST).evaluateAttributeExpressions(flowFile).getValue())) {
            if (acl == null) {
                acl = new AccessControlList();
            }
            acl.grantPermission(grantee, Permission.Read);
        }

        for (final Grantee grantee : createGrantees(context.getProperty(WRITE_USER_LIST).evaluateAttributeExpressions(flowFile).getValue())) {
            if (acl == null) {
                acl = new AccessControlList();
            }
            acl.grantPermission(grantee, Permission.Write);
        }

        for (final Grantee grantee : createGrantees(context.getProperty(READ_ACL_LIST).evaluateAttributeExpressions(flowFile).getValue())) {
            if (acl == null) {
                acl = new AccessControlList();
            }
            acl.grantPermission(grantee, Permission.ReadAcp);
        }

        for (final Grantee grantee : createGrantees(context.getProperty(WRITE_ACL_LIST).evaluateAttributeExpressions(flowFile).getValue())) {
            if (acl == null) {
                acl = new AccessControlList();
            }
            acl.grantPermission(grantee, Permission.WriteAcp);
        }

        return acl;
    }

    protected FlowFile extractExceptionDetails(final Exception e, final ProcessSession session, FlowFile flowFile) {
        flowFile = session.putAttribute(flowFile, "s3.exception", e.getClass().getName());
        if (e instanceof AmazonS3Exception) {
            flowFile = putAttribute(session, flowFile, "s3.additionalDetails", ((AmazonS3Exception) e).getAdditionalDetails());
        }
        if (e instanceof final AmazonServiceException ase) {
            flowFile = putAttribute(session, flowFile, "s3.statusCode", ase.getStatusCode());
            flowFile = putAttribute(session, flowFile, "s3.errorCode", ase.getErrorCode());
            flowFile = putAttribute(session, flowFile, "s3.errorMessage", ase.getErrorMessage());
        }
        return flowFile;
    }

    private FlowFile putAttribute(final ProcessSession session, final FlowFile flowFile, final String key, final Object value) {
        return (value == null) ? flowFile : session.putAttribute(flowFile, key, value.toString());
    }

    /**
     * Create CannedAccessControlList if {@link #CANNED_ACL} property specified.
     *
     * @param context ProcessContext
     * @param flowFile FlowFile
     * @return CannedAccessControlList or null if not specified
     */
    protected final CannedAccessControlList createCannedACL(final ProcessContext context, final FlowFile flowFile) {
        CannedAccessControlList cannedAcl = null;

        final String cannedAclString = context.getProperty(CANNED_ACL).evaluateAttributeExpressions(flowFile).getValue();
        if (!StringUtils.isEmpty(cannedAclString)) {
            cannedAcl = CannedAccessControlList.valueOf(cannedAclString);
        }

        return cannedAcl;
    }

    private boolean isAttributeDefinedRegion(final ProcessContext context) {
        String regionValue = context.getProperty(S3_REGION).getValue();
        return ATTRIBUTE_DEFINED_REGION.getValue().equals(regionValue);
    }
}
