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

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableList;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
@Tags({"google cloud", "google", "storage", "gcs", "fetch"})
@CapabilityDescription("Fetches a file from a Google Cloud Bucket. Designed to be used in tandem with ListGCSBucket.")
@SeeAlso({ListGCSBucket.class, PutGCSObject.class, DeleteGCSObject.class})
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The name of the file, parsed if possible from the " +
                "Content-Disposition response header"),
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
public class FetchGCSObject extends AbstractGCSProcessor {
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
            .defaultValue("${" + CoreAttributes.FILENAME.key() + "}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor GENERATION = new PropertyDescriptor.Builder()
            .name("gcs-generation")
            .displayName("Object Generation")
            .description("The generation of the Object to download. If null, will download latest generation.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor ENCRYPTION_KEY = new PropertyDescriptor.Builder()
            .name("gcs-server-side-encryption-key")
            .displayName("Server Side Encryption Key")
            .description("An AES256 Key (encoded in base64) which the object has been encrypted in.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.<PropertyDescriptor>builder()
                .addAll(super.getSupportedPropertyDescriptors())
                .add(BUCKET)
                .add(KEY)
                .add(GENERATION)
                .add(ENCRYPTION_KEY)
                .build();
    }



    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();

        String bucketName = context.getProperty(BUCKET)
                                   .evaluateAttributeExpressions(flowFile)
                                   .getValue();
        String key = context.getProperty(KEY)
                                   .evaluateAttributeExpressions(flowFile)
                                   .getValue();
        Long generation = context.getProperty(GENERATION)
                                    .evaluateAttributeExpressions(flowFile)
                                    .asLong();
        String encryptionKey = context.getProperty(ENCRYPTION_KEY)
                                    .evaluateAttributeExpressions(flowFile)
                                    .getValue();

        final Storage storage = getCloudService();
        final Map<String, String> attributes = new HashMap<>();
        final BlobId blobId = BlobId.of(bucketName, key, generation);

        try {
            final List<Storage.BlobSourceOption> blobSourceOptions = new ArrayList<>(2);

            if (encryptionKey != null) {
                blobSourceOptions.add(Storage.BlobSourceOption.decryptionKey(encryptionKey));
            }

            if (generation != null) {
                blobSourceOptions.add(Storage.BlobSourceOption.generationMatch());
            }

            final Blob blob = storage.get(blobId);

            if (blob == null) {
                throw new StorageException(404, "Blob " + blobId + " not found");
            }

            final ReadChannel reader = storage.reader(blobId, blobSourceOptions.toArray(new Storage.BlobSourceOption[blobSourceOptions.size()]));

            flowFile = session.importFrom(Channels.newInputStream(reader), flowFile);

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

            if (blob.getContentDisposition() != null) {
                attributes.put(CONTENT_DISPOSITION_ATTR, blob.getContentDisposition());

                final Util.ParsedContentDisposition parsedContentDisposition = Util.parseContentDisposition(blob.getContentDisposition());

                if (parsedContentDisposition != null) {
                    attributes.put(CoreAttributes.FILENAME.key(), parsedContentDisposition.getFileName());
                }
            }

            if (blob.getCreateTime() != null) {
                attributes.put(CREATE_TIME_ATTR, String.valueOf(blob.getCreateTime()));
            }

            if (blob.getUpdateTime() != null) {
                attributes.put(UPDATE_TIME_ATTR, String.valueOf(blob.getUpdateTime()));
            }

        } catch (StorageException e) {
            getLogger().error(e.getMessage(), e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        if (!attributes.isEmpty()) {
            flowFile = session.putAllAttributes(flowFile, attributes);
        }
        session.transfer(flowFile, REL_SUCCESS);

        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        getLogger().info("Successfully retrieved GCS Object for {} in {} millis; routing to success", new Object[]{flowFile, millis});
        session.getProvenanceReporter().fetch(
                flowFile,
                "https://" + bucketName + ".storage.googleapis.com/" + key,
                millis);
    }
}
