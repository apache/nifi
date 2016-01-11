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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.StorageClass;

@SupportsBatching
@SeeAlso({FetchS3Object.class, DeleteS3Object.class})
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"Amazon", "S3", "AWS", "Archive", "Put"})
@CapabilityDescription("Puts FlowFiles to an Amazon S3 Bucket")
@DynamicProperty(name = "The name of a User-Defined Metadata field to add to the S3 Object", value = "The value of a User-Defined Metadata field to add to the S3 Object",
    description = "Allows user-defined metadata to be added to the S3 object as key/value pairs", supportsExpressionLanguage = true)
@ReadsAttribute(attribute = "filename", description = "Uses the FlowFile's filename as the filename for the S3 object")
@WritesAttributes({
    @WritesAttribute(attribute = "s3.bucket", description = "The S3 bucket where the Object was put in S3"),
    @WritesAttribute(attribute = "s3.key", description = "The S3 key within where the Object was put in S3"),
    @WritesAttribute(attribute = "s3.version", description = "The version of the S3 Object that was put to S3"),
    @WritesAttribute(attribute = "s3.etag", description = "The ETag of the S3 Object"),
    @WritesAttribute(attribute = "s3.expiration", description = "A human-readable form of the expiration date of the S3 object, if one is set"),
    @WritesAttribute(attribute = "s3.uploadId", description = "The uploadId used to upload the Object to S3"),
    @WritesAttribute(attribute = "s3.usermetadata", description = "A human-readable form of the User Metadata of the S3 object, if any was set")
})
public class PutS3Object extends AbstractS3Processor {

    public static final PropertyDescriptor EXPIRATION_RULE_ID = new PropertyDescriptor.Builder()
        .name("Expiration Time Rule")
        .required(false)
        .expressionLanguageSupported(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor STORAGE_CLASS = new PropertyDescriptor.Builder()
        .name("Storage Class")
        .required(true)
        .allowableValues(StorageClass.Standard.name(), StorageClass.ReducedRedundancy.name())
        .defaultValue(StorageClass.Standard.name())
        .build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
        Arrays.asList(KEY, BUCKET, ACCESS_KEY, SECRET_KEY, CREDENTIALS_FILE, AWS_CREDENTIALS_PROVIDER_SERVICE, STORAGE_CLASS, REGION, TIMEOUT, EXPIRATION_RULE_ID,
            FULL_CONTROL_USER_LIST, READ_USER_LIST, WRITE_USER_LIST, READ_ACL_LIST, WRITE_ACL_LIST, OWNER, SSL_CONTEXT_SERVICE, ENDPOINT_OVERRIDE));

    final static String S3_BUCKET_KEY = "s3.bucket";
    final static String S3_OBJECT_KEY = "s3.key";
    final static String S3_UPLOAD_ID_ATTR_KEY = "s3.uploadId";
    final static String S3_VERSION_ATTR_KEY = "s3.version";
    final static String S3_ETAG_ATTR_KEY = "s3.etag";
    final static String S3_EXPIRATION_ATTR_KEY = "s3.expiration";
    final static String S3_STORAGECLASS_ATTR_KEY = "s3.storeClass";
    final static String S3_STORAGECLASS_META_KEY = "x-amz-storage-class";
    final static String S3_USERMETA_ATTR_KEY = "s3.usermetadata";

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .dynamic(true)
            .build();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();

        final String bucket = context.getProperty(BUCKET).evaluateAttributeExpressions(flowFile).getValue();
        final String key = context.getProperty(KEY).evaluateAttributeExpressions(flowFile).getValue();

        final AmazonS3Client s3 = getClient();
        final FlowFile ff = flowFile;
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(S3_BUCKET_KEY, bucket);
        attributes.put(S3_OBJECT_KEY, key);

        try {
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream rawIn) throws IOException {
                    try (final InputStream in = new BufferedInputStream(rawIn)) {
                        final ObjectMetadata objectMetadata = new ObjectMetadata();
                        objectMetadata.setContentDisposition(ff.getAttribute(CoreAttributes.FILENAME.key()));
                        objectMetadata.setContentLength(ff.getSize());

                        final String expirationRule = context.getProperty(EXPIRATION_RULE_ID).evaluateAttributeExpressions(ff).getValue();
                        if (expirationRule != null) {
                            objectMetadata.setExpirationTimeRuleId(expirationRule);
                        }

                        final Map<String, String> userMetadata = new HashMap<>();
                        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
                            if (entry.getKey().isDynamic()) {
                                final String value = context.getProperty(entry.getKey()).evaluateAttributeExpressions(ff).getValue();
                                userMetadata.put(entry.getKey().getName(), value);
                            }
                        }

                        if (!userMetadata.isEmpty()) {
                            objectMetadata.setUserMetadata(userMetadata);
                        }

                        final PutObjectRequest request = new PutObjectRequest(bucket, key, in, objectMetadata);
                        request.setStorageClass(StorageClass.valueOf(context.getProperty(STORAGE_CLASS).getValue()));
                        final AccessControlList acl = createACL(context, ff);
                        if (acl != null) {
                            request.setAccessControlList(acl);
                        }

                        final PutObjectResult result = s3.putObject(request);
                        if (result.getVersionId() != null) {
                            attributes.put(S3_VERSION_ATTR_KEY, result.getVersionId());
                        }

                        attributes.put(S3_ETAG_ATTR_KEY, result.getETag());

                        final Date expiration = result.getExpirationTime();
                        if (expiration != null) {
                            attributes.put(S3_EXPIRATION_ATTR_KEY, expiration.toString());
                        }
                        if (result.getMetadata().getRawMetadata().keySet().contains(S3_STORAGECLASS_META_KEY)) {
                            attributes.put(S3_STORAGECLASS_ATTR_KEY,
                                    result.getMetadata().getRawMetadataValue(S3_STORAGECLASS_META_KEY).toString());
                        }
                        if (userMetadata.size() > 0) {
                            List<String> pairs = new ArrayList<String>();
                            for (String userKey : userMetadata.keySet()) {
                                pairs.add(userKey + "=" + userMetadata.get(userKey));
                            }
                            attributes.put(S3_USERMETA_ATTR_KEY, StringUtils.join(pairs, ", "));
                        }
                    }
                }
            });

            if (!attributes.isEmpty()) {
                flowFile = session.putAllAttributes(flowFile, attributes);
            }
            session.transfer(flowFile, REL_SUCCESS);

            final String url = s3.getResourceUrl(bucket, key);
            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().send(flowFile, url, millis);

            getLogger().info("Successfully put {} to Amazon S3 in {} milliseconds", new Object[] {ff, millis});
        } catch (final ProcessException | AmazonClientException pe) {
            getLogger().error("Failed to put {} to Amazon S3 due to {}", new Object[] {flowFile, pe});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
