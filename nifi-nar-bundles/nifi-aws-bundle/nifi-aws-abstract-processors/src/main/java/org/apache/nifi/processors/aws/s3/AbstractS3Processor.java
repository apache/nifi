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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.EmailAddressGrantee;
import com.amazonaws.services.s3.model.Grantee;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.Permission;

public abstract class AbstractS3Processor extends AbstractAWSCredentialsProviderProcessor<AmazonS3Client> {

    public static final PropertyDescriptor FULL_CONTROL_USER_LIST = new PropertyDescriptor.Builder()
            .name("FullControl User List")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("A comma-separated list of Amazon User ID's or E-mail addresses that specifies who should have Full Control for an object")
            .defaultValue("${s3.permissions.full.users}")
            .build();
    public static final PropertyDescriptor READ_USER_LIST = new PropertyDescriptor.Builder()
            .name("Read Permission User List")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("A comma-separated list of Amazon User ID's or E-mail addresses that specifies who should have Read Access for an object")
            .defaultValue("${s3.permissions.read.users}")
            .build();
    public static final PropertyDescriptor WRITE_USER_LIST = new PropertyDescriptor.Builder()
            .name("Write Permission User List")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("A comma-separated list of Amazon User ID's or E-mail addresses that specifies who should have Write Access for an object")
            .defaultValue("${s3.permissions.write.users}")
            .build();
    public static final PropertyDescriptor READ_ACL_LIST = new PropertyDescriptor.Builder()
            .name("Read ACL User List")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("A comma-separated list of Amazon User ID's or E-mail addresses that specifies who should have permissions to read the Access Control List for an object")
            .defaultValue("${s3.permissions.readacl.users}")
            .build();
    public static final PropertyDescriptor WRITE_ACL_LIST = new PropertyDescriptor.Builder()
            .name("Write ACL User List")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("A comma-separated list of Amazon User ID's or E-mail addresses that specifies who should have permissions to change the Access Control List for an object")
            .defaultValue("${s3.permissions.writeacl.users}")
            .build();
    public static final PropertyDescriptor CANNED_ACL = new PropertyDescriptor.Builder()
            .name("canned-acl")
            .displayName("Canned ACL")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Amazon Canned ACL for an object, one of: BucketOwnerFullControl, BucketOwnerRead, LogDeliveryWrite, AuthenticatedRead, PublicReadWrite, PublicRead, Private; " +
                    "will be ignored if any other ACL/permission/owner property is specified")
            .defaultValue("${s3.permissions.cannedacl}")
            .build();
    public static final PropertyDescriptor OWNER = new PropertyDescriptor.Builder()
            .name("Owner")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("The Amazon ID to use for the object's owner")
            .defaultValue("${s3.owner}")
            .build();
    public static final PropertyDescriptor BUCKET = new PropertyDescriptor.Builder()
            .name("Bucket")
            .expressionLanguageSupported(true)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor KEY = new PropertyDescriptor.Builder()
            .name("Object Key")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .defaultValue("${filename}")
            .build();
    public static final PropertyDescriptor SIGNER_OVERRIDE = new PropertyDescriptor.Builder()
            .name("Signer Override")
            .description("The AWS libraries use the default signer but this property allows you to specify a custom signer to support older S3-compatible services.")
            .required(false)
            .allowableValues(
                    new AllowableValue("Default Signature", "Default Signature"),
                    new AllowableValue("AWSS3V4SignerType", "Signature v4"),
                    new AllowableValue("S3SignerType", "Signature v2"))
            .defaultValue("Default Signature")
            .build();
    /**
     * Create client using credentials provider. This is the preferred way for creating clients
     */
    @Override
    protected AmazonS3Client createClient(final ProcessContext context, final AWSCredentialsProvider credentialsProvider, final ClientConfiguration config) {
        getLogger().info("Creating client with credentials provider");

        initializeSignerOverride(context, config);

        final AmazonS3Client s3 = new AmazonS3Client(credentialsProvider, config);

        initalizeEndpointOverride(context, s3);

        return s3;
    }

    private void initalizeEndpointOverride(final ProcessContext context, final AmazonS3Client s3) {
        // if ENDPOINT_OVERRIDE is set, use PathStyleAccess
        if(StringUtils.trimToEmpty(context.getProperty(ENDPOINT_OVERRIDE).evaluateAttributeExpressions().getValue()).isEmpty() == false){
            final S3ClientOptions s3Options = new S3ClientOptions();
            s3Options.setPathStyleAccess(true);
            s3.setS3ClientOptions(s3Options);
        }
    }

    private void initializeSignerOverride(final ProcessContext context, final ClientConfiguration config) {
        String signer = context.getProperty(SIGNER_OVERRIDE).getValue();

        if (signer != null && !signer.equals(SIGNER_OVERRIDE.getDefaultValue())) {
            config.setSignerOverride(signer);
        }
    }

    /**
     * Create client using AWSCredentials
     *
     * @deprecated use {@link #createClient(ProcessContext, AWSCredentialsProvider, ClientConfiguration)} instead
     */
    @Override
    protected AmazonS3Client createClient(final ProcessContext context, final AWSCredentials credentials, final ClientConfiguration config) {
        getLogger().info("Creating client with AWS credentials");

        initializeSignerOverride(context, config);

        final AmazonS3Client s3 = new AmazonS3Client(credentials, config);

        initalizeEndpointOverride(context, s3);

        return s3;
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

    protected String getUrlForObject(final String bucket, final String key) {
        Region region = getRegion();

        if (region == null) {
            return  DEFAULT_PROTOCOL.toString() + "://s3.amazonaws.com/" + bucket + "/" + key;
        } else {
            final String endpoint = region.getServiceEndpoint("s3");
            return DEFAULT_PROTOCOL.toString() + "://" + endpoint + "/" + bucket + "/" + key;
        }
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
            if (acl == null) {
                acl = new AccessControlList();
            }
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
}
