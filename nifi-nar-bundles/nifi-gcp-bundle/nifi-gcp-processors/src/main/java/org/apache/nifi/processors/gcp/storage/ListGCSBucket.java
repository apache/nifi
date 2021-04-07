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

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableList;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

/**
 * List objects in a google cloud storage bucket by object name pattern.
 */
@PrimaryNodeOnly
@TriggerSerially
@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"google cloud", "google", "storage", "gcs", "list"})
@CapabilityDescription("Retrieves a listing of objects from an GCS bucket. For each object that is listed, creates a FlowFile that represents "
        + "the object so that it can be fetched in conjunction with FetchGCSObject. This Processor is designed to run on Primary Node only "
        + "in a cluster. If the primary node changes, the new Primary Node will pick up where the previous node left off without duplicating "
        + "all of the data.")
@Stateful(scopes = Scope.CLUSTER, description = "After performing a listing of keys, the timestamp of the newest key is stored, "
        + "along with the keys that share that same timestamp. This allows the Processor to list only keys that have been added or modified after "
        + "this date the next time that the Processor is run. State is stored across the cluster so that this Processor can be run on Primary Node only and if a new Primary "
        + "Node is selected, the new node can pick up where the previous node left off, without duplicating the data.")
@SeeAlso({PutGCSObject.class, DeleteGCSObject.class, FetchGCSObject.class})
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The name of the file"),
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
public class ListGCSBucket extends AbstractGCSProcessor {
    public static final PropertyDescriptor BUCKET = new PropertyDescriptor
            .Builder().name("gcs-bucket")
            .displayName("Bucket")
            .description(BUCKET_DESC)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor PREFIX = new PropertyDescriptor.Builder()
            .name("gcs-prefix")
            .displayName("Prefix")
            .description("The prefix used to filter the object list. In most cases, it should end with a forward slash ('/').")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor USE_GENERATIONS = new PropertyDescriptor.Builder()
            .name("gcs-use-generations")
            .displayName("Use Generations")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .description("Specifies whether to use GCS Generations, if applicable.  If false, only the latest version of each object will be returned.")
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
        .name("record-writer")
        .displayName("Record Writer")
        .description("Specifies the Record Writer to use for creating the listing. If not specified, one FlowFile will be created for each entity that is listed. If the Record Writer is specified, " +
            "all entities will be written to a single FlowFile instead of adding attributes to individual FlowFiles.")
        .required(false)
        .identifiesControllerService(RecordSetWriterFactory.class)
        .build();


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.<PropertyDescriptor>builder()
            .add(BUCKET)
            .add(RECORD_WRITER)
            .addAll(super.getSupportedPropertyDescriptors())
            .add(PREFIX)
            .add(USE_GENERATIONS)
            .build();
    }

    private static final Set<Relationship> relationships = Collections.singleton(REL_SUCCESS);

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    // State tracking
    public static final String CURRENT_TIMESTAMP = "currentTimestamp";
    public static final String CURRENT_KEY_PREFIX = "key-";
    private volatile long currentTimestamp = 0L;
    private final Set<String> currentKeys = Collections.synchronizedSet(new HashSet<>());


    private Set<String> extractKeys(final StateMap stateMap) {
        return stateMap.toMap().entrySet().parallelStream()
                .filter(x -> x.getKey().startsWith(CURRENT_KEY_PREFIX))
                .map(Map.Entry::getValue)
                .collect(Collectors.toSet());
    }

    void restoreState(final ProcessSession session) throws IOException {
        final StateMap stateMap = session.getState(Scope.CLUSTER);
        if (stateMap.getVersion() == -1L || stateMap.get(CURRENT_TIMESTAMP) == null || stateMap.get(CURRENT_KEY_PREFIX+"0") == null) {
            currentTimestamp = 0L;
            currentKeys.clear();
        } else {
            currentTimestamp = Long.parseLong(stateMap.get(CURRENT_TIMESTAMP));
            currentKeys.clear();
            currentKeys.addAll(extractKeys(stateMap));
        }
    }

    void persistState(final ProcessSession session, final long timestamp, final Set<String> keys) {
        final Map<String, String> state = new HashMap<>();
        state.put(CURRENT_TIMESTAMP, String.valueOf(timestamp));

        int i = 0;
        for (final String key : keys) {
            state.put(CURRENT_KEY_PREFIX+i, key);
            i++;
        }

        try {
            session.setState(state, Scope.CLUSTER);
        } catch (IOException ioe) {
            getLogger().error("Failed to save cluster-wide state. If NiFi is restarted, data duplication may occur", ioe);
        }
    }

    Set<String> getStateKeys() {
        return Collections.unmodifiableSet(currentKeys);
    }

    long getStateTimestamp() {
        return currentTimestamp;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        try {
            restoreState(session);
        } catch (IOException e) {
            getLogger().error("Failed to restore processor state; yielding", e);
            context.yield();
            return;
        }

        final long startNanos = System.nanoTime();

        final String bucket = context.getProperty(BUCKET).evaluateAttributeExpressions().getValue();
        final String prefix = context.getProperty(PREFIX).evaluateAttributeExpressions().getValue();
        final boolean useGenerations = context.getProperty(USE_GENERATIONS).asBoolean();

        final List<Storage.BlobListOption> listOptions = new ArrayList<>();
        if (prefix != null) {
            listOptions.add(Storage.BlobListOption.prefix(prefix));
        }
        if (useGenerations) {
            listOptions.add(Storage.BlobListOption.versions(true));
        }

        final Storage storage = getCloudService();

        long maxTimestamp = 0L;
        final Set<String> keysMatchingTimestamp = new HashSet<>();

        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        final BlobWriter writer;
        if (writerFactory == null) {
            writer = new AttributeBlobWriter(session);
        } else {
            writer = new RecordBlobWriter(session, writerFactory, getLogger());
        }

        try {
            writer.beginListing();

            Page<Blob> blobPage = storage.list(bucket, listOptions.toArray(new Storage.BlobListOption[0]));
            int listCount = 0;

            do {
                for (final Blob blob : blobPage.getValues()) {
                    long lastModified = blob.getUpdateTime();
                    if (lastModified < currentTimestamp || lastModified == currentTimestamp && currentKeys.contains(blob.getName())) {
                        continue;
                    }

                    writer.addToListing(blob);

                    // Update state
                    if (lastModified > maxTimestamp) {
                        maxTimestamp = lastModified;
                        keysMatchingTimestamp.clear();
                    }
                    if (lastModified == maxTimestamp) {
                        keysMatchingTimestamp.add(blob.getName());
                    }

                    listCount++;
                }

                if (writer.isCheckpoint()) {
                    commit(session, listCount, maxTimestamp, keysMatchingTimestamp);
                    listCount = 0;
                }

                blobPage = blobPage.getNextPage();
            } while (blobPage != null);

            writer.finishListing();

            if (maxTimestamp == 0) {
                getLogger().debug("No new objects in GCS bucket {} to list. Yielding.", bucket);
                context.yield();
            } else {
                commit(session, listCount, maxTimestamp, keysMatchingTimestamp);
            }
        } catch (final Exception e) {
            getLogger().error("Failed to list contents of GCS Bucket due to {}", new Object[] {e}, e);
            writer.finishListingExceptionally(e);
            session.rollback();
            context.yield();
            return;
        }

        final long listMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        getLogger().info("Successfully listed GCS bucket {} in {} millis", new Object[]{bucket, listMillis});
    }

    private void commit(final ProcessSession session, final int listCount, final long timestamp, final Set<String> keysMatchingTimestamp) {
        if (listCount > 0) {
            currentTimestamp = timestamp;
            currentKeys.clear();
            currentKeys.addAll(keysMatchingTimestamp);
            persistState(session, currentTimestamp, currentKeys);

            getLogger().info("Successfully listed {} new files from GCS; routing to success", new Object[] {listCount});
            session.commitAsync();
        }
    }


    private interface BlobWriter {
        void beginListing() throws IOException, SchemaNotFoundException;

        void addToListing(Blob blob) throws IOException;

        void finishListing() throws IOException;

        void finishListingExceptionally(Exception cause);

        boolean isCheckpoint();
    }


    static class RecordBlobWriter implements BlobWriter {
        private static final RecordSchema RECORD_SCHEMA;

        public static final String BUCKET = "bucket";
        public static final String NAME = "name";
        public static final String SIZE = "size";
        public static final String CACHE_CONTROL = "cacheControl";
        public static final String COMPONENT_COUNT = "componentCount";
        public static final String CONTENT_DISPOSITION = "contentDisposition";
        public static final String CONTENT_ENCODING = "contentEncoding";
        public static final String CONTENT_LANGUAGE = "contentLanguage";
        public static final String CRC32C = "crc32c";
        public static final String CREATE_TIME = "createTime";
        public static final String UPDATE_TIME = "updateTime";
        public static final String ENCRYPTION_ALGORITHM = "encryptionAlgorithm";
        public static final String ENCRYPTION_KEY_SHA256 = "encryptionKeySha256";
        public static final String ETAG = "etag";
        public static final String GENERATED_ID = "generatedId";
        public static final String GENERATION = "generation";
        public static final String MD5 = "md5";
        public static final String MEDIA_LINK = "mediaLink";
        public static final String METAGENERATION = "metageneration";
        public static final String OWNER = "owner";
        public static final String OWNER_TYPE = "ownerType";
        public static final String URI = "uri";

        static {
            final List<RecordField> fields = new ArrayList<>();
            fields.add(new RecordField(BUCKET, RecordFieldType.STRING.getDataType(), false));
            fields.add(new RecordField(NAME, RecordFieldType.STRING.getDataType(), false));
            fields.add(new RecordField(SIZE, RecordFieldType.LONG.getDataType()));
            fields.add(new RecordField(CACHE_CONTROL, RecordFieldType.STRING.getDataType()));
            fields.add(new RecordField(COMPONENT_COUNT, RecordFieldType.INT.getDataType()));
            fields.add(new RecordField(CONTENT_DISPOSITION, RecordFieldType.LONG.getDataType()));
            fields.add(new RecordField(CONTENT_ENCODING, RecordFieldType.STRING.getDataType()));
            fields.add(new RecordField(CONTENT_LANGUAGE, RecordFieldType.STRING.getDataType()));
            fields.add(new RecordField(CRC32C, RecordFieldType.STRING.getDataType()));
            fields.add(new RecordField(CREATE_TIME, RecordFieldType.TIMESTAMP.getDataType()));
            fields.add(new RecordField(UPDATE_TIME, RecordFieldType.TIMESTAMP.getDataType()));
            fields.add(new RecordField(ENCRYPTION_ALGORITHM, RecordFieldType.STRING.getDataType()));
            fields.add(new RecordField(ENCRYPTION_KEY_SHA256, RecordFieldType.STRING.getDataType()));
            fields.add(new RecordField(ETAG, RecordFieldType.STRING.getDataType()));
            fields.add(new RecordField(GENERATED_ID, RecordFieldType.STRING.getDataType()));
            fields.add(new RecordField(GENERATION, RecordFieldType.LONG.getDataType()));
            fields.add(new RecordField(MD5, RecordFieldType.STRING.getDataType()));
            fields.add(new RecordField(MEDIA_LINK, RecordFieldType.STRING.getDataType()));
            fields.add(new RecordField(METAGENERATION, RecordFieldType.LONG.getDataType()));
            fields.add(new RecordField(OWNER, RecordFieldType.STRING.getDataType()));
            fields.add(new RecordField(OWNER_TYPE, RecordFieldType.STRING.getDataType()));
            fields.add(new RecordField(URI, RecordFieldType.STRING.getDataType()));

            RECORD_SCHEMA = new SimpleRecordSchema(fields);
        }


        private final ProcessSession session;
        private final RecordSetWriterFactory writerFactory;
        private final ComponentLog logger;
        private RecordSetWriter recordWriter;
        private FlowFile flowFile;

        public RecordBlobWriter(final ProcessSession session, final RecordSetWriterFactory writerFactory, final ComponentLog logger) {
            this.session = session;
            this.writerFactory = writerFactory;
            this.logger = logger;
        }

        @Override
        public void beginListing() throws IOException, SchemaNotFoundException {
            flowFile = session.create();

            final OutputStream out = session.write(flowFile);
            recordWriter = writerFactory.createWriter(logger, RECORD_SCHEMA, out, flowFile);
            recordWriter.beginRecordSet();
        }

        @Override
        public void addToListing(final Blob blob) throws IOException {
            recordWriter.write(createRecordForListing(blob));
        }

        @Override
        public void finishListing() throws IOException {
            final WriteResult writeResult = recordWriter.finishRecordSet();
            recordWriter.close();

            if (writeResult.getRecordCount() == 0) {
                session.remove(flowFile);
            } else {
                final Map<String, String> attributes = new HashMap<>(writeResult.getAttributes());
                attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                flowFile = session.putAllAttributes(flowFile, attributes);

                session.transfer(flowFile, REL_SUCCESS);
            }
        }

        @Override
        public void finishListingExceptionally(final Exception cause) {
            try {
                recordWriter.close();
            } catch (IOException e) {
                logger.error("Failed to write listing as Records due to {}", new Object[] {e}, e);
            }

            session.remove(flowFile);
        }

        @Override
        public boolean isCheckpoint() {
            return false;
        }

        private Record createRecordForListing(final Blob blob) {
            final Map<String, Object> values = new HashMap<>();
            values.put(BUCKET, blob.getBucket());
            values.put(NAME, blob.getName());
            values.put(SIZE, blob.getSize());
            values.put(CACHE_CONTROL, blob.getCacheControl());
            values.put(COMPONENT_COUNT, blob.getComponentCount());
            values.put(CONTENT_DISPOSITION, blob.getContentDisposition());
            values.put(CONTENT_ENCODING, blob.getContentEncoding());
            values.put(CONTENT_LANGUAGE, blob.getContentLanguage());
            values.put(CRC32C, blob.getCrc32c());
            values.put(CREATE_TIME, blob.getCreateTime() == null ? null : new Timestamp(blob.getCreateTime()));
            values.put(UPDATE_TIME, blob.getUpdateTime() == null ? null : new Timestamp(blob.getUpdateTime()));

            final BlobInfo.CustomerEncryption encryption = blob.getCustomerEncryption();
            if (encryption != null) {
                values.put(ENCRYPTION_ALGORITHM, encryption.getEncryptionAlgorithm());
                values.put(ENCRYPTION_KEY_SHA256, encryption.getKeySha256());
            }

            values.put(ETAG, blob.getEtag());
            values.put(GENERATED_ID, blob.getGeneratedId());
            values.put(GENERATION, blob.getGeneration());
            values.put(MD5, blob.getMd5());
            values.put(MEDIA_LINK, blob.getMediaLink());
            values.put(METAGENERATION, blob.getMetageneration());

            final Acl.Entity owner = blob.getOwner();
            if (owner != null) {
                if (owner instanceof Acl.User) {
                    values.put(OWNER, ((Acl.User) owner).getEmail());
                    values.put(OWNER_TYPE, "user");
                } else if (owner instanceof Acl.Group) {
                    values.put(OWNER, ((Acl.Group) owner).getEmail());
                    values.put(OWNER_TYPE, "group");
                } else if (owner instanceof Acl.Domain) {
                    values.put(OWNER, ((Acl.Domain) owner).getDomain());
                    values.put(OWNER_TYPE, "domain");
                } else if (owner instanceof Acl.Project) {
                    values.put(OWNER, ((Acl.Project) owner).getProjectId());
                    values.put(OWNER_TYPE, "project");
                }
            }

            values.put(URI, blob.getSelfLink());

            return new MapRecord(RECORD_SCHEMA, values);
        }
    }



    /**
     * Writes Blobs by creating a new FlowFile for each blob and writing information as FlowFile attributes
     */
    private static class AttributeBlobWriter implements BlobWriter {
        private final ProcessSession session;

        public AttributeBlobWriter(final ProcessSession session) {
            this.session = session;
        }

        @Override
        public void beginListing() {
        }

        @Override
        public void addToListing(final Blob blob) {
            final Map<String, String> attributes = StorageAttributes.createAttributes(blob);

            // Create the flowfile
            FlowFile flowFile = session.create();
            flowFile = session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, REL_SUCCESS);
        }

        @Override
        public void finishListing() {
        }

        @Override
        public void finishListingExceptionally(final Exception cause) {
        }

        @Override
        public boolean isCheckpoint() {
            return true;
        }
    }
}
