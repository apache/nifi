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
package org.apache.nifi.processors.gcp.storage;

import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableList;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.cloud.storage.Storage.PredefinedAcl.ALL_AUTHENTICATED_USERS;
import static com.google.cloud.storage.Storage.PredefinedAcl.AUTHENTICATED_READ;
import static com.google.cloud.storage.Storage.PredefinedAcl.BUCKET_OWNER_FULL_CONTROL;
import static com.google.cloud.storage.Storage.PredefinedAcl.BUCKET_OWNER_READ;
import static com.google.cloud.storage.Storage.PredefinedAcl.PRIVATE;
import static com.google.cloud.storage.Storage.PredefinedAcl.PROJECT_PRIVATE;
import static com.google.cloud.storage.Storage.PredefinedAcl.PUBLIC_READ;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.BUCKET_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.BUCKET_DESC;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.CACHE_CONTROL_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.CACHE_CONTROL_DESC;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.COMPONENT_COUNT_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.COMPONENT_COUNT_DESC;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.CONTENT_DISPOSITION_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.CONTENT_DISPOSITION_DESC;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.CONTENT_ENCODING_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.CONTENT_ENCODING_DESC;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.CONTENT_LANGUAGE_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.CONTENT_LANGUAGE_DESC;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.CRC32C_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.CRC32C_DESC;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.CREATE_TIME_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.CREATE_TIME_DESC;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.ENCRYPTION_ALGORITHM_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.ENCRYPTION_ALGORITHM_DESC;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.ENCRYPTION_SHA256_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.ENCRYPTION_SHA256_DESC;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.ETAG_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.ETAG_DESC;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.GENERATED_ID_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.GENERATED_ID_DESC;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.GENERATION_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.GENERATION_DESC;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.KEY_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.KEY_DESC;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.MD5_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.MD5_DESC;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.MEDIA_LINK_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.MEDIA_LINK_DESC;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.METAGENERATION_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.METAGENERATION_DESC;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.OWNER_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.OWNER_DESC;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.OWNER_TYPE_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.OWNER_TYPE_DESC;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.SIZE_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.SIZE_DESC;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.UPDATE_TIME_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.UPDATE_TIME_DESC;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.URI_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.URI_DESC;


@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"google", "google cloud", "gcs", "archive", "put"})
@CapabilityDescription("Puts flow files to a Google Cloud Bucket.")
@SeeAlso({FetchGCSObject.class, DeleteGCSObject.class, ListGCSBucket.class})
@DynamicProperty(name = "The name of a User-Defined Metadata field to add to the GCS Object",
        value = "The value of a User-Defined Metadata field to add to the GCS Object",
        description = "Allows user-defined metadata to be added to the GCS object as key/value pairs",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
@ReadsAttributes({
        @ReadsAttribute(attribute = "filename", description = "Uses the FlowFile's filename as the filename for the " +
                "GCS object"),
        @ReadsAttribute(attribute = "mime.type", description = "Uses the FlowFile's MIME type as the content-type for " +
                "the GCS object")
})
@WritesAttributes({
        @WritesAttribute(attribute = BUCKET_ATTR, description = BUCKET_DESC),
        @WritesAttribute(attribute = KEY_ATTR, description = KEY_DESC),
        @WritesAttribute(attribute = SIZE_ATTR, description = SIZE_DESC),
        @WritesAttribute(attribute = CACHE_CONTROL_ATTR, description = CACHE_CONTROL_DESC),
        @WritesAttribute(attribute = COMPONENT_COUNT_ATTR, description = COMPONENT_COUNT_DESC),
        @WritesAttribute(attribute = CONTENT_DISPOSITION_ATTR, description = CONTENT_DISPOSITION_DESC),
        @WritesAttribute(attribute = CONTENT_ENCODING_ATTR, description = CONTENT_ENCODING_DESC),
        @WritesAttribute(attribute = CONTENT_LANGUAGE_ATTR, description = CONTENT_LANGUAGE_DESC),
        @WritesAttribute(attribute = "mime.type", description = "The MIME/Content-Type of the object"),
        @WritesAttribute(attribute = CRC32C_ATTR, description = CRC32C_DESC),
        @WritesAttribute(attribute = CREATE_TIME_ATTR, description = CREATE_TIME_DESC),
        @WritesAttribute(attribute = UPDATE_TIME_ATTR, description = UPDATE_TIME_DESC),
        @WritesAttribute(attribute = ENCRYPTION_ALGORITHM_ATTR, description = ENCRYPTION_ALGORITHM_DESC),
        @WritesAttribute(attribute = ENCRYPTION_SHA256_ATTR, description = ENCRYPTION_SHA256_DESC),
        @WritesAttribute(attribute = ETAG_ATTR, description = ETAG_DESC),
        @WritesAttribute(attribute = GENERATED_ID_ATTR, description = GENERATED_ID_DESC),
        @WritesAttribute(attribute = GENERATION_ATTR, description = GENERATION_DESC),
        @WritesAttribute(attribute = MD5_ATTR, description = MD5_DESC),
        @WritesAttribute(attribute = MEDIA_LINK_ATTR, description = MEDIA_LINK_DESC),
        @WritesAttribute(attribute = METAGENERATION_ATTR, description = METAGENERATION_DESC),
        @WritesAttribute(attribute = OWNER_ATTR, description = OWNER_DESC),
        @WritesAttribute(attribute = OWNER_TYPE_ATTR, description = OWNER_TYPE_DESC),
        @WritesAttribute(attribute = URI_ATTR, description = URI_DESC)
})
public class PutGCSObject extends AbstractGCSProcessor {
    public static final PropertyDescriptor BUCKET = new PropertyDescriptor
            .Builder().name("gcs-bucket")
            .displayName("Bucket")
            .description(BUCKET_DESC)
            .required(true)
            .defaultValue("${" + BUCKET_ATTR + "}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEY = new PropertyDescriptor
            .Builder().name("gcs-key")
            .displayName("Key")
            .description(KEY_DESC)
            .required(true)
            .defaultValue("${filename}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONTENT_TYPE = new PropertyDescriptor
            .Builder().name("gcs-content-type")
                      .displayName("Content Type")
                      .description("Content Type for the file, i.e. text/plain")
                      .defaultValue("${mime.type}")
                      .required(false)
                      .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                      .build();

    public static final PropertyDescriptor MD5 = new PropertyDescriptor
            .Builder().name("gcs-object-md5")
            .displayName("MD5 Hash")
            .description("MD5 Hash (encoded in Base64) of the file for server-side validation.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final PropertyDescriptor CRC32C = new PropertyDescriptor
            .Builder().name("gcs-object-crc32c")
            .displayName("CRC32C Checksum")
            .description("CRC32C Checksum (encoded in Base64, big-Endian order) of the file for server-side validation.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final AllowableValue ACL_ALL_AUTHENTICATED_USERS = new AllowableValue(
            ALL_AUTHENTICATED_USERS.name(), "All Authenticated Users", "Gives the bucket or object owner OWNER " +
            "permission, and gives all authenticated Google account holders READER and WRITER permissions. " +
            "All other permissions are removed."
    );

    public static final AllowableValue ACL_AUTHENTICATED_READ = new AllowableValue(
            AUTHENTICATED_READ.name(), "Authenticated Read", "Gives the bucket or object owner OWNER permission, " +
            "and gives all authenticated Google account holders READER permission. All other permissions are removed."
    );

    public static final AllowableValue ACL_BUCKET_OWNER_FULL_CONTROL = new AllowableValue(
            BUCKET_OWNER_FULL_CONTROL.name(), "Bucket Owner Full Control", "Gives the object and bucket owners OWNER " +
            "permission. All other permissions are removed."
    );

    public static final AllowableValue ACL_BUCKET_OWNER_READ = new AllowableValue(
            BUCKET_OWNER_READ.name(), "Bucket Owner Read Only", "Gives the object owner OWNER permission, and gives " +
            "the bucket owner READER permission. All other permissions are removed."
    );

    public static final AllowableValue ACL_PRIVATE = new AllowableValue(
            PRIVATE.name(), "Private", "Gives the bucket or object owner OWNER permission for a bucket or object, " +
            "and removes all other access permissions."
    );

    public static final AllowableValue ACL_PROJECT_PRIVATE = new AllowableValue(
            PROJECT_PRIVATE.name(), "Project Private", "Gives permission to the project team based on their roles. " +
            "Anyone who is part of the team has READER permission. Project owners and project editors have OWNER " +
            "permission. This is the default ACL for newly created buckets. This is also the default ACL for newly " +
            "created objects unless the default object ACL for that bucket has been changed."
    );

    public static final AllowableValue ACL_PUBLIC_READ = new AllowableValue(
            PUBLIC_READ.name(), "Public Read Only", "Gives the bucket or object owner OWNER permission, and gives all " +
            "users, both authenticated and anonymous, READER permission. When you apply this to an object, anyone on " +
            "the Internet can read the object without authenticating."
    );

    public static final PropertyDescriptor ACL = new PropertyDescriptor.Builder()
            .name("gcs-object-acl")
            .displayName("Object ACL")
            .description("Access Control to be attached to the object uploaded. Not providing this will revert to bucket defaults.")
            .required(false)
            .allowableValues(
                    ACL_ALL_AUTHENTICATED_USERS,
                    ACL_AUTHENTICATED_READ,
                    ACL_BUCKET_OWNER_FULL_CONTROL,
                    ACL_BUCKET_OWNER_READ,
                    ACL_PRIVATE,
                    ACL_PROJECT_PRIVATE,
                    ACL_PUBLIC_READ)
            .build();

    public static final PropertyDescriptor ENCRYPTION_KEY = new PropertyDescriptor.Builder()
            .name("gcs-server-side-encryption-key")
            .displayName("Server Side Encryption Key")
            .description("An AES256 Encryption Key (encoded in base64) for server-side encryption of the object.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();


    public static final PropertyDescriptor OVERWRITE = new PropertyDescriptor.Builder()
            .name("gcs-overwrite-object")
            .displayName("Overwrite Object")
            .description("If false, the upload to GCS will succeed only if the object does not exist.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final AllowableValue CD_INLINE = new AllowableValue(
            "inline", "Inline", "Indicates that the object should be loaded and rendered within the browser."
    );

    public static final AllowableValue CD_ATTACHMENT = new AllowableValue(
            "attachment", "Attachment", "Indicates that the object should be saved (using a Save As... dialog) rather " +
            "than opened directly within the browser"
    );

    public static final PropertyDescriptor CONTENT_DISPOSITION_TYPE = new PropertyDescriptor.Builder()
            .name("gcs-content-disposition-type")
            .displayName("Content Disposition Type")
            .description("Type of RFC-6266 Content Disposition to be attached to the object")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(CD_INLINE, CD_ATTACHMENT)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.<PropertyDescriptor>builder()
                .addAll(super.getSupportedPropertyDescriptors())
                .add(BUCKET)
                .add(KEY)
                .add(CONTENT_TYPE)
                .add(MD5)
                .add(CRC32C)
                .add(ACL)
                .add(ENCRYPTION_KEY)
                .add(OVERWRITE)
                .add(CONTENT_DISPOSITION_TYPE)
                .build();
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();

        final String bucket = context.getProperty(BUCKET)
                .evaluateAttributeExpressions(flowFile)
                .getValue();
        final String key = context.getProperty(KEY)
                .evaluateAttributeExpressions(flowFile)
                .getValue();
        final boolean overwrite = context.getProperty(OVERWRITE).asBoolean();

        final FlowFile ff = flowFile;
        final String ffFilename = ff.getAttributes().get(CoreAttributes.FILENAME.key());
        final Map<String, String> attributes = new HashMap<>();

        try {
            final Storage storage = getCloudService();
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream rawIn) throws IOException {
                    try (final InputStream in = new BufferedInputStream(rawIn)) {
                        final BlobId id = BlobId.of(bucket, key);
                        final BlobInfo.Builder blobInfoBuilder = BlobInfo.newBuilder(id);
                        final List<Storage.BlobWriteOption> blobWriteOptions = new ArrayList<>();

                        if (!overwrite) {
                            blobWriteOptions.add(Storage.BlobWriteOption.doesNotExist());
                        }

                        final String contentDispositionType = context.getProperty(CONTENT_DISPOSITION_TYPE).getValue();
                        if (contentDispositionType != null) {
                            blobInfoBuilder.setContentDisposition(contentDispositionType + "; filename=" + ffFilename);
                        }

                        final String contentType = context.getProperty(CONTENT_TYPE)
                                .evaluateAttributeExpressions(ff).getValue();
                        if (contentType != null) {
                            blobInfoBuilder.setContentType(contentType);
                        }

                        final String md5 = context.getProperty(MD5)
                                .evaluateAttributeExpressions(ff).getValue();
                        if (md5 != null) {
                            blobInfoBuilder.setMd5(md5);
                            blobWriteOptions.add(Storage.BlobWriteOption.md5Match());
                        }

                        final String crc32c = context.getProperty(CRC32C)
                                .evaluateAttributeExpressions(ff).getValue();
                        if (crc32c != null) {
                            blobInfoBuilder.setCrc32c(crc32c);
                            blobWriteOptions.add(Storage.BlobWriteOption.crc32cMatch());
                        }

                        final String acl = context.getProperty(ACL).getValue();
                        if (acl != null) {
                            blobWriteOptions.add(Storage.BlobWriteOption.predefinedAcl(
                                    Storage.PredefinedAcl.valueOf(acl)
                            ));
                        }

                        final String encryptionKey = context.getProperty(ENCRYPTION_KEY)
                                .evaluateAttributeExpressions(ff).getValue();
                        if (encryptionKey != null) {
                            blobWriteOptions.add(Storage.BlobWriteOption.encryptionKey(encryptionKey));
                        }

                        final HashMap<String, String> userMetadata = new HashMap<>();
                        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
                            if (entry.getKey().isDynamic()) {
                                final String value = context.getProperty(
                                        entry.getKey()).evaluateAttributeExpressions(ff).getValue();
                                userMetadata.put(entry.getKey().getName(), value);
                            }
                        }

                        if (!userMetadata.isEmpty()) {
                            blobInfoBuilder.setMetadata(userMetadata);
                        }

                        try {

                            final Blob blob = storage.create(blobInfoBuilder.build(),
                                    in,
                                    blobWriteOptions.toArray(new Storage.BlobWriteOption[blobWriteOptions.size()])
                            );

                            // Create attributes
                            attributes.put(BUCKET_ATTR, blob.getBucket());
                            attributes.put(KEY_ATTR, blob.getName());


                            if (blob.getSize() != null) {
                                attributes.put(SIZE_ATTR, String.valueOf(blob.getSize()));
                            }

                            if (blob.getCacheControl() != null) {
                                attributes.put(CACHE_CONTROL_ATTR, blob.getCacheControl());
                            }

                            if (blob.getComponentCount() != null) {
                                attributes.put(COMPONENT_COUNT_ATTR, String.valueOf(blob.getComponentCount()));
                            }

                            if (blob.getContentDisposition() != null) {
                                attributes.put(CONTENT_DISPOSITION_ATTR, blob.getContentDisposition());
                                final Util.ParsedContentDisposition parsed = Util.parseContentDisposition(blob.getContentDisposition());

                                if (parsed != null) {
                                    attributes.put(CoreAttributes.FILENAME.key(), parsed.getFileName());
                                }
                            }

                            if (blob.getContentEncoding() != null) {
                                attributes.put(CONTENT_ENCODING_ATTR, blob.getContentEncoding());
                            }

                            if (blob.getContentLanguage() != null) {
                                attributes.put(CONTENT_LANGUAGE_ATTR, blob.getContentLanguage());
                            }

                            if (blob.getContentType() != null) {
                                attributes.put(CoreAttributes.MIME_TYPE.key(), blob.getContentType());
                            }

                            if (blob.getCrc32c() != null) {
                                attributes.put(CRC32C_ATTR, blob.getCrc32c());
                            }

                            if (blob.getCustomerEncryption() != null) {
                                final BlobInfo.CustomerEncryption encryption = blob.getCustomerEncryption();

                                attributes.put(ENCRYPTION_ALGORITHM_ATTR, encryption.getEncryptionAlgorithm());
                                attributes.put(ENCRYPTION_SHA256_ATTR, encryption.getKeySha256());
                            }

                            if (blob.getEtag() != null) {
                                attributes.put(ETAG_ATTR, blob.getEtag());
                            }

                            if (blob.getGeneratedId() != null) {
                                attributes.put(GENERATED_ID_ATTR, blob.getGeneratedId());
                            }

                            if (blob.getGeneration() != null) {
                                attributes.put(GENERATION_ATTR, String.valueOf(blob.getGeneration()));
                            }

                            if (blob.getMd5() != null) {
                                attributes.put(MD5_ATTR, blob.getMd5());
                            }

                            if (blob.getMediaLink() != null) {
                                attributes.put(MEDIA_LINK_ATTR, blob.getMediaLink());
                            }

                            if (blob.getMetageneration() != null) {
                                attributes.put(METAGENERATION_ATTR, String.valueOf(blob.getMetageneration()));
                            }

                            if (blob.getOwner() != null) {
                                final Acl.Entity entity = blob.getOwner();

                                if (entity instanceof Acl.User) {
                                    attributes.put(OWNER_ATTR, ((Acl.User) entity).getEmail());
                                    attributes.put(OWNER_TYPE_ATTR, "user");
                                } else if (entity instanceof Acl.Group) {
                                    attributes.put(OWNER_ATTR, ((Acl.Group) entity).getEmail());
                                    attributes.put(OWNER_TYPE_ATTR, "group");
                                } else if (entity instanceof Acl.Domain) {
                                    attributes.put(OWNER_ATTR, ((Acl.Domain) entity).getDomain());
                                    attributes.put(OWNER_TYPE_ATTR, "domain");
                                } else if (entity instanceof Acl.Project) {
                                    attributes.put(OWNER_ATTR, ((Acl.Project) entity).getProjectId());
                                    attributes.put(OWNER_TYPE_ATTR, "project");
                                }
                            }

                            if (blob.getSelfLink() != null) {
                                attributes.put(URI_ATTR, blob.getSelfLink());
                            }

                            if (blob.getCreateTime() != null) {
                                attributes.put(CREATE_TIME_ATTR, String.valueOf(blob.getCreateTime()));
                            }

                            if (blob.getUpdateTime() != null) {
                                attributes.put(UPDATE_TIME_ATTR, String.valueOf(blob.getUpdateTime()));
                            }
                        } catch (StorageException e) {
                            getLogger().error("Failure completing upload flowfile={} bucket={} key={} reason={}",
                                    new Object[]{ffFilename, bucket, key, e.getMessage()}, e);
                            throw (e);
                        }


                    }
                }
            });

            if (!attributes.isEmpty()) {
                flowFile = session.putAllAttributes(flowFile, attributes);
            }
            session.transfer(flowFile, REL_SUCCESS);
            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            final String url = "https://" + bucket + ".storage.googleapis.com/" + key;

            session.getProvenanceReporter().send(flowFile, url, millis);
            getLogger().info("Successfully put {} to Google Cloud Storage in {} milliseconds",
                    new Object[]{ff, millis});

        } catch (final ProcessException | StorageException e) {
            getLogger().error("Failed to put {} to Google Cloud Storage due to {}", new Object[]{flowFile, e.getMessage()}, e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
