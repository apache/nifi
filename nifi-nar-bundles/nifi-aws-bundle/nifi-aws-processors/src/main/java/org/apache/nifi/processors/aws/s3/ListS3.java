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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.internal.Constants;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.model.VersionListing;
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
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@PrimaryNodeOnly
@TriggerSerially
@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"Amazon", "S3", "AWS", "list"})
@CapabilityDescription("Retrieves a listing of objects from an S3 bucket. For each object that is listed, creates a FlowFile that represents "
        + "the object so that it can be fetched in conjunction with FetchS3Object. This Processor is designed to run on Primary Node only "
        + "in a cluster. If the primary node changes, the new Primary Node will pick up where the previous node left off without duplicating "
        + "all of the data.")
@Stateful(scopes = Scope.CLUSTER, description = "After performing a listing of keys, the timestamp of the newest key is stored, "
        + "along with the keys that share that same timestamp. This allows the Processor to list only keys that have been added or modified after "
        + "this date the next time that the Processor is run. State is stored across the cluster so that this Processor can be run on Primary Node only and if a new Primary "
        + "Node is selected, the new node can pick up where the previous node left off, without duplicating the data.")
@WritesAttributes({
        @WritesAttribute(attribute = "s3.bucket", description = "The name of the S3 bucket"),
        @WritesAttribute(attribute = "filename", description = "The name of the file"),
        @WritesAttribute(attribute = "s3.etag", description = "The ETag that can be used to see if the file has changed"),
        @WritesAttribute(attribute = "s3.isLatest", description = "A boolean indicating if this is the latest version of the object"),
        @WritesAttribute(attribute = "s3.lastModified", description = "The last modified time in milliseconds since epoch in UTC time"),
        @WritesAttribute(attribute = "s3.length", description = "The size of the object in bytes"),
        @WritesAttribute(attribute = "s3.storeClass", description = "The storage class of the object"),
        @WritesAttribute(attribute = "s3.version", description = "The version of the object, if applicable"),
        @WritesAttribute(attribute = "s3.tag.___", description = "If 'Write Object Tags' is set to 'True', the tags associated to the S3 object that is being listed " +
                "will be written as part of the flowfile attributes"),
        @WritesAttribute(attribute = "s3.user.metadata.___", description = "If 'Write User Metadata' is set to 'True', the user defined metadata associated to the S3 object that is being listed " +
                "will be written as part of the flowfile attributes")})
@SeeAlso({FetchS3Object.class, PutS3Object.class, DeleteS3Object.class})
public class ListS3 extends AbstractS3Processor {

    public static final PropertyDescriptor DELIMITER = new Builder()
            .name("delimiter")
            .displayName("Delimiter")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("The string used to delimit directories within the bucket. Please consult the AWS documentation " +
                    "for the correct use of this field.")
            .build();

    public static final PropertyDescriptor PREFIX = new Builder()
            .name("prefix")
            .displayName("Prefix")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("The prefix used to filter the object list. In most cases, it should end with a forward slash ('/').")
            .build();

    public static final PropertyDescriptor USE_VERSIONS = new Builder()
            .name("use-versions")
            .displayName("Use Versions")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .description("Specifies whether to use S3 versions, if applicable.  If false, only the latest version of each object will be returned.")
            .build();

    public static final PropertyDescriptor LIST_TYPE = new Builder()
            .name("list-type")
            .displayName("List Type")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .allowableValues(
                    new AllowableValue("1", "List Objects V1"),
                    new AllowableValue("2", "List Objects V2"))
            .defaultValue("1")
            .description("Specifies whether to use the original List Objects or the newer List Objects Version 2 endpoint.")
            .build();

    public static final PropertyDescriptor MIN_AGE = new Builder()
            .name("min-age")
            .displayName("Minimum Object Age")
            .description("The minimum age that an S3 object must be in order to be considered; any object younger than this amount of time (according to last modification date) will be ignored")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("0 sec")
            .build();

    public static final PropertyDescriptor WRITE_OBJECT_TAGS = new Builder()
            .name("write-s3-object-tags")
            .displayName("Write Object Tags")
            .description("If set to 'True', the tags associated with the S3 object will be written as FlowFile attributes")
            .required(true)
            .allowableValues(new AllowableValue("true", "True"), new AllowableValue("false", "False"))
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor REQUESTER_PAYS = new Builder()
            .name("requester-pays")
            .displayName("Requester Pays")
            .required(true)
            .description("If true, indicates that the requester consents to pay any charges associated with listing "
                    + "the S3 bucket.  This sets the 'x-amz-request-payer' header to 'requester'.  Note that this "
                    + "setting is not applicable when 'Use Versions' is 'true'.")
            .addValidator(createRequesterPaysValidator())
            .allowableValues(new AllowableValue("true", "True", "Indicates that the requester consents to pay any charges associated "
                    + "with listing the S3 bucket."), new AllowableValue("false", "False", "Does not consent to pay "
                            + "requester charges for listing the S3 bucket."))
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor WRITE_USER_METADATA = new Builder()
            .name("write-s3-user-metadata")
            .displayName("Write User Metadata")
            .description("If set to 'True', the user defined metadata associated with the S3 object will be added to FlowFile attributes/records")
            .required(true)
            .allowableValues(new AllowableValue("true", "True"), new AllowableValue("false", "False"))
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new Builder()
        .name("record-writer")
        .displayName("Record Writer")
        .description("Specifies the Record Writer to use for creating the listing. If not specified, one FlowFile will be created for each entity that is listed. If the Record Writer is specified, " +
            "all entities will be written to a single FlowFile instead of adding attributes to individual FlowFiles.")
        .required(false)
        .identifiesControllerService(RecordSetWriterFactory.class)
        .build();

    static final PropertyDescriptor BATCH_SIZE = new Builder()
        .name("Listing Batch Size")
        .displayName("Listing Batch Size")
        .description("If not using a Record Writer, this property dictates how many S3 objects should be listed in a single batch. Once this number is reached, the FlowFiles that have been created " +
            "will be transferred out of the Processor. Setting this value lower may result in lower latency by sending out the FlowFiles before the complete listing has finished. However, it can " +
            "significantly reduce performance. Larger values may take more memory to store all of the information before sending the FlowFiles out. This property is ignored if using a Record " +
            "Writer, as one of the main benefits of the Record Writer is being able to emit the entire listing as a single FlowFile.")
        .required(false)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .defaultValue("100")
        .build();


    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(Arrays.asList(
        BUCKET,
        REGION,
        ACCESS_KEY,
        SECRET_KEY,
        RECORD_WRITER,
        MIN_AGE,
        BATCH_SIZE,
        WRITE_OBJECT_TAGS,
        WRITE_USER_METADATA,
        CREDENTIALS_FILE,
        AWS_CREDENTIALS_PROVIDER_SERVICE,
        TIMEOUT,
        SSL_CONTEXT_SERVICE,
        ENDPOINT_OVERRIDE,
        SIGNER_OVERRIDE,
        PROXY_CONFIGURATION_SERVICE,
        PROXY_HOST,
        PROXY_HOST_PORT,
        PROXY_USERNAME,
        PROXY_PASSWORD,
        DELIMITER,
        PREFIX,
        USE_VERSIONS,
        LIST_TYPE,
        REQUESTER_PAYS));

    public static final Set<Relationship> relationships = Collections.singleton(REL_SUCCESS);

    public static final String CURRENT_TIMESTAMP = "currentTimestamp";
    public static final String CURRENT_KEY_PREFIX = "key-";

    // State tracking
    private final AtomicReference<ListingSnapshot> listing = new AtomicReference<>(new ListingSnapshot(0L, Collections.emptySet()));

    private static Validator createRequesterPaysValidator() {
        return new Validator() {
            @Override
            public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
                boolean requesterPays = Boolean.valueOf(input);
                boolean useVersions = context.getProperty(USE_VERSIONS).asBoolean();
                boolean valid = !requesterPays || !useVersions;
                return new ValidationResult.Builder()
                        .input(input)
                        .subject(subject)
                        .valid(valid)
                        .explanation(valid ? null : "'Requester Pays' cannot be used when listing object versions.")
                        .build();
            }
        };
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    private Set<String> extractKeys(final StateMap stateMap) {
        Set<String> keys = new HashSet<>();
        for (Map.Entry<String, String>  entry : stateMap.toMap().entrySet()) {
            if (entry.getKey().startsWith(CURRENT_KEY_PREFIX)) {
                keys.add(entry.getValue());
            }
        }
        return keys;
    }

    private void restoreState(final ProcessSession session) throws IOException {
        final StateMap stateMap = session.getState(Scope.CLUSTER);
        if (stateMap.getVersion() == -1L || stateMap.get(CURRENT_TIMESTAMP) == null || stateMap.get(CURRENT_KEY_PREFIX+"0") == null) {
            forcefullyUpdateListing(0L, Collections.emptySet());
        } else {
            final long timestamp = Long.parseLong(stateMap.get(CURRENT_TIMESTAMP));
            final Set<String> keys = extractKeys(stateMap);
            forcefullyUpdateListing(timestamp, keys);
        }
    }

    private void updateListingIfNewer(final long timestamp, final Set<String> keys) {
        final ListingSnapshot updatedListing = new ListingSnapshot(timestamp, keys);
        listing.getAndUpdate(current -> current.getTimestamp() > timestamp ? current : updatedListing);
    }

    private void forcefullyUpdateListing(final long timestamp, final Set<String> keys) {
        final ListingSnapshot updatedListing = new ListingSnapshot(timestamp, keys);
        listing.set(updatedListing);
    }

    private void persistState(final ProcessSession session, final long timestamp, final Collection<String> keys) {
        final Map<String, String> state = new HashMap<>();
        state.put(CURRENT_TIMESTAMP, String.valueOf(timestamp));

        int i = 0;
        for (final String key : keys) {
            state.put(CURRENT_KEY_PREFIX + i, key);
            i++;
        }

        try {
            session.setState(state, Scope.CLUSTER);
        } catch (IOException ioe) {
            getLogger().error("Failed to save cluster-wide state. If NiFi is restarted, data duplication may occur", ioe);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        try {
            restoreState(session);
        } catch (IOException ioe) {
            getLogger().error("Failed to restore processor state; yielding", ioe);
            context.yield();
            return;
        }

        final long startNanos = System.nanoTime();
        final String bucket = context.getProperty(BUCKET).evaluateAttributeExpressions().getValue();
        final long minAgeMilliseconds = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final long listingTimestamp = System.currentTimeMillis();
        final boolean requesterPays = context.getProperty(REQUESTER_PAYS).asBoolean();
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();

        final ListingSnapshot currentListing = listing.get();
        final long currentTimestamp = currentListing.getTimestamp();
        final Set<String> currentKeys = currentListing.getKeys();

        final AmazonS3 client = getClient();
        int listCount = 0;
        int totalListCount = 0;
        long latestListedTimestampInThisCycle = currentTimestamp;
        String delimiter = context.getProperty(DELIMITER).getValue();
        String prefix = context.getProperty(PREFIX).evaluateAttributeExpressions().getValue();

        boolean useVersions = context.getProperty(USE_VERSIONS).asBoolean();
        int listType = context.getProperty(LIST_TYPE).asInteger();
        S3BucketLister bucketLister = useVersions
                ? new S3VersionBucketLister(client)
                : listType == 2
                    ? new S3ObjectBucketListerVersion2(client)
                    : new S3ObjectBucketLister(client);

        bucketLister.setBucketName(bucket);
        bucketLister.setRequesterPays(requesterPays);

        if (delimiter != null && !delimiter.isEmpty()) {
            bucketLister.setDelimiter(delimiter);
        }
        if (prefix != null && !prefix.isEmpty()) {
            bucketLister.setPrefix(prefix);
        }

        VersionListing versionListing;
        final Set<String> listedKeys = new HashSet<>();
        getLogger().trace("Start listing, listingTimestamp={}, currentTimestamp={}, currentKeys={}", new Object[]{listingTimestamp, currentTimestamp, currentKeys});

        final S3ObjectWriter writer;
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        if (writerFactory == null) {
            writer = new AttributeObjectWriter(session);
        } else {
            writer = new RecordObjectWriter(session, writerFactory, getLogger());
        }

        try {
            writer.beginListing();

                do {
                    versionListing = bucketLister.listVersions();
                    for (S3VersionSummary versionSummary : versionListing.getVersionSummaries()) {
                        long lastModified = versionSummary.getLastModified().getTime();
                        if (lastModified < currentTimestamp
                            || lastModified == currentTimestamp && currentKeys.contains(versionSummary.getKey())
                            || lastModified > (listingTimestamp - minAgeMilliseconds)) {
                            continue;
                        }

                        getLogger().trace("Listed key={}, lastModified={}, currentKeys={}", new Object[]{versionSummary.getKey(), lastModified, currentKeys});

                        // Get object tags if configured to do so
                        GetObjectTaggingResult taggingResult = null;
                        if (context.getProperty(WRITE_OBJECT_TAGS).asBoolean()) {
                            try {
                                taggingResult = client.getObjectTagging(new GetObjectTaggingRequest(versionSummary.getBucketName(), versionSummary.getKey()));
                            } catch (final Exception e) {
                                getLogger().warn("Failed to obtain Object Tags for S3 Object {} in bucket {}. Will list S3 Object without the object tags",
                                    new Object[] {versionSummary.getKey(), versionSummary.getBucketName()}, e);
                            }
                        }

                        // Get user metadata if configured to do so
                        ObjectMetadata objectMetadata = null;
                        if (context.getProperty(WRITE_USER_METADATA).asBoolean()) {
                            try {
                                objectMetadata = client.getObjectMetadata(new GetObjectMetadataRequest(versionSummary.getBucketName(), versionSummary.getKey()));
                            } catch (final Exception e) {
                                getLogger().warn("Failed to obtain User Metadata for S3 Object {} in bucket {}. Will list S3 Object without the user metadata",
                                    new Object[] {versionSummary.getKey(), versionSummary.getBucketName()}, e);
                            }
                        }

                        // Write the entity to the listing
                        writer.addToListing(versionSummary, taggingResult, objectMetadata);

                        // Track the latest lastModified timestamp and keys having that timestamp.
                        // NOTE: Amazon S3 lists objects in UTF-8 character encoding in lexicographical order. Not ordered by timestamps.
                        if (lastModified > latestListedTimestampInThisCycle) {
                            latestListedTimestampInThisCycle = lastModified;
                            listedKeys.clear();
                            listedKeys.add(versionSummary.getKey());

                        } else if (lastModified == latestListedTimestampInThisCycle) {
                            listedKeys.add(versionSummary.getKey());
                        }

                        listCount++;
                    }
                    bucketLister.setNextMarker();

                    totalListCount += listCount;

                    if (listCount >= batchSize && writer.isCheckpoint()) {
                        getLogger().info("Successfully listed {} new files from S3; routing to success", new Object[] {listCount});
                        session.commitAsync();
                    }

                    listCount = 0;
                } while (bucketLister.isTruncated());

                writer.finishListing();
        } catch (final Exception e) {
            getLogger().error("Failed to list contents of bucket due to {}", new Object[] {e}, e);
            writer.finishListingExceptionally(e);
            session.rollback();
            context.yield();
            return;
        }

        final Set<String> updatedKeys = new HashSet<>();
        if (latestListedTimestampInThisCycle <= currentTimestamp) {
            updatedKeys.addAll(currentKeys);
        }
        updatedKeys.addAll(listedKeys);

        persistState(session, latestListedTimestampInThisCycle, updatedKeys);

        final long latestListed = latestListedTimestampInThisCycle;
        session.commitAsync(() -> {
            updateListingIfNewer(latestListed, updatedKeys);
        });

        final long listMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        getLogger().info("Successfully listed S3 bucket {} in {} millis", new Object[]{bucket, listMillis});

        if (totalListCount == 0) {
            getLogger().debug("No new objects in S3 bucket {} to list. Yielding.", new Object[]{bucket});
            context.yield();
        }
    }


    private interface S3BucketLister {
        void setBucketName(String bucketName);
        void setPrefix(String prefix);
        void setDelimiter(String delimiter);
        void setRequesterPays(boolean requesterPays);
        // Versions have a superset of the fields that Objects have, so we'll use
        // them as a common interface
        VersionListing listVersions();
        void setNextMarker();
        boolean isTruncated();
    }

    public class S3ObjectBucketLister implements S3BucketLister {
        private AmazonS3 client;
        private ListObjectsRequest listObjectsRequest;
        private ObjectListing objectListing;

        public S3ObjectBucketLister(AmazonS3 client) {
            this.client = client;
        }

        @Override
        public void setBucketName(String bucketName) {
            listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName);
        }

        @Override
        public void setPrefix(String prefix) {
            listObjectsRequest.setPrefix(prefix);
        }

        @Override
        public void setDelimiter(String delimiter) {
            listObjectsRequest.setDelimiter(delimiter);
        }

        @Override
        public void setRequesterPays(boolean requesterPays) {
            listObjectsRequest.setRequesterPays(requesterPays);
        }

        @Override
        public VersionListing listVersions() {
            VersionListing versionListing = new VersionListing();
            this.objectListing = client.listObjects(listObjectsRequest);
            for(S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                S3VersionSummary versionSummary = new S3VersionSummary();
                versionSummary.setBucketName(objectSummary.getBucketName());
                versionSummary.setETag(objectSummary.getETag());
                versionSummary.setKey(objectSummary.getKey());
                versionSummary.setLastModified(objectSummary.getLastModified());
                versionSummary.setOwner(objectSummary.getOwner());
                versionSummary.setSize(objectSummary.getSize());
                versionSummary.setStorageClass(objectSummary.getStorageClass());
                versionSummary.setIsLatest(true);

                versionListing.getVersionSummaries().add(versionSummary);
            }

            return versionListing;
        }

        @Override
        public void setNextMarker() {
            listObjectsRequest.setMarker(objectListing.getNextMarker());
        }

        @Override
        public boolean isTruncated() {
            return (objectListing == null) ? false : objectListing.isTruncated();
        }
    }

    public class S3ObjectBucketListerVersion2 implements S3BucketLister {
        private AmazonS3 client;
        private ListObjectsV2Request listObjectsRequest;
        private ListObjectsV2Result objectListing;

        public S3ObjectBucketListerVersion2(AmazonS3 client) {
            this.client = client;
        }

        @Override
        public void setBucketName(String bucketName) {
            listObjectsRequest = new ListObjectsV2Request().withBucketName(bucketName);
        }

        @Override
        public void setPrefix(String prefix) {
            listObjectsRequest.setPrefix(prefix);
        }

        @Override
        public void setDelimiter(String delimiter) {
            listObjectsRequest.setDelimiter(delimiter);
        }

        @Override
        public void setRequesterPays(boolean requesterPays) {
            listObjectsRequest.setRequesterPays(requesterPays);
        }

        @Override
        public VersionListing listVersions() {
            VersionListing versionListing = new VersionListing();
            this.objectListing = client.listObjectsV2(listObjectsRequest);
            for(S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                S3VersionSummary versionSummary = new S3VersionSummary();
                versionSummary.setBucketName(objectSummary.getBucketName());
                versionSummary.setETag(objectSummary.getETag());
                versionSummary.setKey(objectSummary.getKey());
                versionSummary.setLastModified(objectSummary.getLastModified());
                versionSummary.setOwner(objectSummary.getOwner());
                versionSummary.setSize(objectSummary.getSize());
                versionSummary.setStorageClass(objectSummary.getStorageClass());
                versionSummary.setIsLatest(true);

                versionListing.getVersionSummaries().add(versionSummary);
            }

            return versionListing;
        }

        @Override
        public void setNextMarker() {
            listObjectsRequest.setContinuationToken(objectListing.getNextContinuationToken());
        }

        @Override
        public boolean isTruncated() {
            return (objectListing == null) ? false : objectListing.isTruncated();
        }
    }

    public class S3VersionBucketLister implements S3BucketLister {
        private AmazonS3 client;
        private ListVersionsRequest listVersionsRequest;
        private VersionListing versionListing;

        public S3VersionBucketLister(AmazonS3 client) {
            this.client = client;
        }

        @Override
        public void setBucketName(String bucketName) {
            listVersionsRequest = new ListVersionsRequest().withBucketName(bucketName);
        }

        @Override
        public void setPrefix(String prefix) {
            listVersionsRequest.setPrefix(prefix);
        }

        @Override
        public void setDelimiter(String delimiter) {
            listVersionsRequest.setDelimiter(delimiter);
        }

        @Override
        public void setRequesterPays(boolean requesterPays) {
            // Not supported in versionListing, so this does nothing.
        }

        @Override
        public VersionListing listVersions() {
            versionListing = client.listVersions(listVersionsRequest);
            return versionListing;
        }

        @Override
        public void setNextMarker() {
            listVersionsRequest.setKeyMarker(versionListing.getNextKeyMarker());
            listVersionsRequest.setVersionIdMarker(versionListing.getNextVersionIdMarker());
        }

        @Override
        public boolean isTruncated() {
            return (versionListing == null) ? false : versionListing.isTruncated();
        }
    }

    interface S3ObjectWriter {
        void beginListing() throws IOException, SchemaNotFoundException;

        void addToListing(S3VersionSummary summary, GetObjectTaggingResult taggingResult, ObjectMetadata objectMetadata) throws IOException;

        void finishListing() throws IOException;

        void finishListingExceptionally(Exception cause);

        boolean isCheckpoint();
    }

    static class RecordObjectWriter implements S3ObjectWriter {
        private static final RecordSchema RECORD_SCHEMA;

        private static final String KEY = "key";
        private static final String BUCKET = "bucket";
        private static final String OWNER = "owner";
        private static final String ETAG = "etag";
        private static final String LAST_MODIFIED = "lastModified";
        private static final String SIZE = "size";
        private static final String STORAGE_CLASS = "storageClass";
        private static final String IS_LATEST = "latest";
        private static final String VERSION_ID = "versionId";
        private static final String TAGS = "tags";
        private static final String USER_METADATA = "userMetadata";

        static {
            final List<RecordField> fields = new ArrayList<>();
            fields.add(new RecordField(KEY, RecordFieldType.STRING.getDataType(), false));
            fields.add(new RecordField(BUCKET, RecordFieldType.STRING.getDataType(), false));
            fields.add(new RecordField(OWNER, RecordFieldType.STRING.getDataType(), true));
            fields.add(new RecordField(ETAG, RecordFieldType.STRING.getDataType(), false));
            fields.add(new RecordField(LAST_MODIFIED, RecordFieldType.TIMESTAMP.getDataType(), false));
            fields.add(new RecordField(SIZE, RecordFieldType.LONG.getDataType(), false));
            fields.add(new RecordField(STORAGE_CLASS, RecordFieldType.STRING.getDataType(), false));
            fields.add(new RecordField(IS_LATEST, RecordFieldType.BOOLEAN.getDataType(), false));
            fields.add(new RecordField(VERSION_ID, RecordFieldType.STRING.getDataType(), true));
            fields.add(new RecordField(TAGS, RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()), true));
            fields.add(new RecordField(USER_METADATA, RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()), true));

            RECORD_SCHEMA = new SimpleRecordSchema(fields);
        }


        private final ProcessSession session;
        private final RecordSetWriterFactory writerFactory;
        private final ComponentLog logger;
        private RecordSetWriter recordWriter;
        private FlowFile flowFile;

        public RecordObjectWriter(final ProcessSession session, final RecordSetWriterFactory writerFactory, final ComponentLog logger) {
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
        public void addToListing(final S3VersionSummary summary, final GetObjectTaggingResult taggingResult, final ObjectMetadata objectMetadata) throws IOException {
            recordWriter.write(createRecordForListing(summary, taggingResult, objectMetadata));
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

        private Record createRecordForListing(final S3VersionSummary versionSummary, final GetObjectTaggingResult taggingResult, final ObjectMetadata objectMetadata) {
            final Map<String, Object> values = new HashMap<>();
            values.put(KEY, versionSummary.getKey());
            values.put(BUCKET, versionSummary.getBucketName());

            if (versionSummary.getOwner() != null) { // We may not have permission to read the owner
                values.put(OWNER, versionSummary.getOwner().getId());
            }

            values.put(ETAG, versionSummary.getETag());
            values.put(LAST_MODIFIED, new Timestamp(versionSummary.getLastModified().getTime()));
            values.put(SIZE, versionSummary.getSize());
            values.put(STORAGE_CLASS, versionSummary.getStorageClass());
            values.put(IS_LATEST, versionSummary.isLatest());
            final String versionId = versionSummary.getVersionId();
            if (versionId != null && !versionId.equals(Constants.NULL_VERSION_ID)) {
                values.put(VERSION_ID, versionSummary.getVersionId());
            }

            if (taggingResult != null) {
                final Map<String, String> tags = new HashMap<>();
                taggingResult.getTagSet().forEach(tag -> {
                    tags.put(tag.getKey(), tag.getValue());
                });

                values.put(TAGS, tags);
            }

            if (objectMetadata != null) {
                values.put(USER_METADATA, objectMetadata.getUserMetadata());
            }

            return new MapRecord(RECORD_SCHEMA, values);
        }
    }



    static class AttributeObjectWriter implements S3ObjectWriter {
        private final ProcessSession session;

        public AttributeObjectWriter(final ProcessSession session) {
            this.session = session;
        }

        @Override
        public void beginListing() {
        }

        @Override
        public void addToListing(final S3VersionSummary versionSummary, final GetObjectTaggingResult taggingResult, final ObjectMetadata objectMetadata) {
            // Create the attributes
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.FILENAME.key(), versionSummary.getKey());
            attributes.put("s3.bucket", versionSummary.getBucketName());
            if (versionSummary.getOwner() != null) { // We may not have permission to read the owner
                attributes.put("s3.owner", versionSummary.getOwner().getId());
            }
            attributes.put("s3.etag", versionSummary.getETag());
            attributes.put("s3.lastModified", String.valueOf(versionSummary.getLastModified().getTime()));
            attributes.put("s3.length", String.valueOf(versionSummary.getSize()));
            attributes.put("s3.storeClass", versionSummary.getStorageClass());
            attributes.put("s3.isLatest", String.valueOf(versionSummary.isLatest()));
            if (versionSummary.getVersionId() != null) {
                attributes.put("s3.version", versionSummary.getVersionId());
            }

            if (taggingResult != null) {
                final List<Tag> tags = taggingResult.getTagSet();
                for (final Tag tag : tags) {
                    attributes.put("s3.tag." + tag.getKey(), tag.getValue());
                }
            }

            if (objectMetadata != null) {
                for (Map.Entry<String, String> e : objectMetadata.getUserMetadata().entrySet()) {
                    attributes.put("s3.user.metadata." + e.getKey(), e.getValue());
                }
            }

            // Create the flowfile
            FlowFile flowFile = session.create();
            flowFile = session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, REL_SUCCESS);
        }

        @Override
        public void finishListing() throws IOException {
        }

        @Override
        public void finishListingExceptionally(final Exception cause) {
        }

        @Override
        public boolean isCheckpoint() {
            return true;
        }
    }

    private static class ListingSnapshot {
        private final long timestamp;
        private final Set<String> keys;

        public ListingSnapshot(final long timestamp, final Set<String> keys) {
            this.timestamp = timestamp;
            this.keys = keys;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public Set<String> getKeys() {
            return keys;
        }
    }
}
