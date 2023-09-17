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
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
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
        @WritesAttribute(attribute = "filename", description = "The name of the file, parsed if possible from the Content-Disposition response header"),
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
            .displayName("Name")
            .description(KEY_DESC)
            .required(true)
            .defaultValue("${" + CoreAttributes.FILENAME.key() + "}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor GENERATION = new PropertyDescriptor.Builder()
            .name("gcs-generation")
            .displayName("Object Generation")
            .description("The generation of the Object to download. If not set, the latest generation will be downloaded.")
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

    public static final PropertyDescriptor RANGE_START = new PropertyDescriptor.Builder()
            .name("gcs-object-range-start")
            .displayName("Range Start")
            .description("The byte position at which to start reading from the object. An empty value or a value of " +
                    "zero will start reading at the beginning of the object.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final PropertyDescriptor RANGE_LENGTH = new PropertyDescriptor.Builder()
            .name("gcs-object-range-length")
            .displayName("Range Length")
            .description("The number of bytes to download from the object, starting from the Range Start. An empty " +
                    "value or a value that extends beyond the end of the object will read to the end of the object.")
            .addValidator(StandardValidators.createDataSizeBoundsValidator(1, Long.MAX_VALUE))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(BUCKET);
        descriptors.add(KEY);
        descriptors.addAll(super.getSupportedPropertyDescriptors());
        descriptors.add(GENERATION);
        descriptors.add(ENCRYPTION_KEY);
        descriptors.add(RANGE_START);
        descriptors.add(RANGE_LENGTH);
        return Collections.unmodifiableList(descriptors);
    }

    @Override
    protected List<String> getRequiredPermissions() {
        return Collections.singletonList("storage.objects.get");
    }

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog verificationLogger, final Map<String, String> attributes) {
        final List<ConfigVerificationResult> results = new ArrayList<>(super.verify(context, verificationLogger, attributes));

        final String bucketName = context.getProperty(BUCKET).evaluateAttributeExpressions(attributes).getValue();
        final String key = context.getProperty(KEY).evaluateAttributeExpressions(attributes).getValue();

        final Storage storage = getCloudService(context);

        try {
            final FetchedBlob blob = fetchBlob(context, storage, attributes);

            final CountingOutputStream out = new CountingOutputStream(NullOutputStream.NULL_OUTPUT_STREAM);
            IOUtils.copy(blob.contents, out);
            final long byteCount = out.getByteCount();
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Fetch GCS Blob")
                    .outcome(Outcome.SUCCESSFUL)
                    .explanation(String.format("Successfully fetched [%s] from Bucket [%s], totaling %s bytes", key, bucketName, byteCount))
                    .build());
        } catch (final StorageException | IOException e) {
            getLogger().error(String.format("Failed to fetch [%s] from Bucket [%s]", key, bucketName), e);
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Fetch GCS Blob")
                    .outcome(Outcome.FAILED)
                    .explanation(String.format("Failed to fetch [%s] from Bucket [%s]: %s", key, bucketName, e.getMessage()))
                    .build());
        }

        return results;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();

        final String bucketName = context.getProperty(BUCKET).evaluateAttributeExpressions(flowFile).getValue();
        final String key = context.getProperty(KEY).evaluateAttributeExpressions(flowFile).getValue();

        final Storage storage = getCloudService();

        final long rangeStart = (context.getProperty(RANGE_START).isSet() ? context.getProperty(RANGE_START).evaluateAttributeExpressions(flowFile).asDataSize(DataUnit.B).longValue() : 0L);
        final Long rangeLength = (context.getProperty(RANGE_LENGTH).isSet() ? context.getProperty(RANGE_LENGTH).evaluateAttributeExpressions(flowFile).asDataSize(DataUnit.B).longValue() : null);

        try {
            final FetchedBlob blob = fetchBlob(context, storage, flowFile.getAttributes());
            flowFile = session.importFrom(blob.contents, flowFile);

            final Map<String, String> attributes = StorageAttributes.createAttributes(blob.blob);
            flowFile = session.putAllAttributes(flowFile, attributes);
        } catch (final StorageException | IOException e) {
            getLogger().error("Failed to fetch GCS Object due to {}", e, e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        session.transfer(flowFile, REL_SUCCESS);

        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        getLogger().info("Successfully retrieved GCS Object for {} in {} millis; routing to success", new Object[]{flowFile, millis});

        final String transitUri = getTransitUri(storage.getOptions().getHost(), bucketName, key);
        session.getProvenanceReporter().fetch(flowFile, transitUri, millis);
    }

    private FetchedBlob fetchBlob(final ProcessContext context, final Storage storage, final Map<String, String> attributes) throws IOException {
        final String bucketName = context.getProperty(BUCKET).evaluateAttributeExpressions(attributes).getValue();
        final String key = context.getProperty(KEY).evaluateAttributeExpressions(attributes).getValue();
        final Long generation = context.getProperty(GENERATION).evaluateAttributeExpressions(attributes).asLong();
        final long rangeStart = (context.getProperty(RANGE_START).isSet() ? context.getProperty(RANGE_START).evaluateAttributeExpressions(attributes).asDataSize(DataUnit.B).longValue() : 0L);
        final Long rangeLength = (context.getProperty(RANGE_LENGTH).isSet() ? context.getProperty(RANGE_LENGTH).evaluateAttributeExpressions(attributes).asDataSize(DataUnit.B).longValue() : null);

        final BlobId blobId = BlobId.of(bucketName, key, generation);
        final List<Storage.BlobSourceOption> blobSourceOptions = getBlobSourceOptions(context, attributes);

        if (blobId.getName() == null || blobId.getName().isEmpty()) {
            throw new IllegalArgumentException("Name is required");
        }
        final Blob blob = storage.get(blobId);
        if (blob == null) {
            throw new StorageException(404, "Blob " + blobId + " not found");
        }
        if (rangeStart > 0 && rangeStart >= blob.getSize()) {
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("Start position: {}, blob size: {}", new Object[] {rangeStart, blob.getSize()});
            }
            throw new StorageException(416, "The range specified is not valid for the blob " + blob.getBlobId()
                    + ". Range Start is beyond the end of the blob.");
        }

        final ReadChannel reader = storage.reader(blob.getBlobId(), blobSourceOptions.toArray(new Storage.BlobSourceOption[0]));
        reader.seek(rangeStart);

        final InputStream rawInputStream = Channels.newInputStream(reader);
        final InputStream blobContents = rangeLength == null ? rawInputStream : new BoundedInputStream(rawInputStream, rangeLength);

        return new FetchedBlob(blobContents, blob);
    }

    private List<Storage.BlobSourceOption> getBlobSourceOptions(final ProcessContext context, final Map<String, String> attributes) {
        final Long generation = context.getProperty(GENERATION).evaluateAttributeExpressions(attributes).asLong();
        final String encryptionKey = context.getProperty(ENCRYPTION_KEY).evaluateAttributeExpressions(attributes).getValue();

        final List<Storage.BlobSourceOption> blobSourceOptions = new ArrayList<>(2);

        if (encryptionKey != null) {
            blobSourceOptions.add(Storage.BlobSourceOption.decryptionKey(encryptionKey));
        }

        if (generation != null) {
            blobSourceOptions.add(Storage.BlobSourceOption.generationMatch());
        }
        return blobSourceOptions;
    }

    private class FetchedBlob {
        private final InputStream contents;
        private final Blob blob;

        private FetchedBlob(final InputStream contents, final Blob blob) {
            this.contents = contents;
            this.blob = blob;
        }
    }
}
