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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.Tag;
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
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;

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
                "will be written as part of the flowfile attributes")})
@SeeAlso({FetchS3Object.class, PutS3Object.class, DeleteS3Object.class})
public class ListS3 extends AbstractS3Processor {

    public static final PropertyDescriptor DELIMITER = new PropertyDescriptor.Builder()
            .name("delimiter")
            .displayName("Delimiter")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("The string used to delimit directories within the bucket. Please consult the AWS documentation " +
                    "for the correct use of this field.")
            .build();

    public static final PropertyDescriptor PREFIX = new PropertyDescriptor.Builder()
            .name("prefix")
            .displayName("Prefix")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("The prefix used to filter the object list. In most cases, it should end with a forward slash ('/').")
            .build();

    public static final PropertyDescriptor USE_VERSIONS = new PropertyDescriptor.Builder()
            .name("use-versions")
            .displayName("Use Versions")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .description("Specifies whether to use S3 versions, if applicable.  If false, only the latest version of each object will be returned.")
            .build();

    public static final PropertyDescriptor LIST_TYPE = new PropertyDescriptor.Builder()
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

    public static final PropertyDescriptor MIN_AGE = new PropertyDescriptor.Builder()
            .name("min-age")
            .displayName("Minimum Object Age")
            .description("The minimum age that an S3 object must be in order to be considered; any object younger than this amount of time (according to last modification date) will be ignored")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("0 sec")
            .build();

    public static final PropertyDescriptor WRITE_OBJECT_TAGS = new PropertyDescriptor.Builder()
            .name("write-s3-object-tags")
            .displayName("Write Object Tags")
            .description("If set to 'True', the tags associated with the S3 object will be written as FlowFile attributes")
            .required(true)
            .allowableValues(new AllowableValue("true", "True"), new AllowableValue("false", "False"))
            .defaultValue("false")
            .build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(BUCKET, REGION, ACCESS_KEY, SECRET_KEY, WRITE_OBJECT_TAGS, CREDENTIALS_FILE,
                    AWS_CREDENTIALS_PROVIDER_SERVICE, TIMEOUT, SSL_CONTEXT_SERVICE, ENDPOINT_OVERRIDE,
                    SIGNER_OVERRIDE, PROXY_CONFIGURATION_SERVICE, PROXY_HOST, PROXY_HOST_PORT, PROXY_USERNAME,
                    PROXY_PASSWORD, DELIMITER, PREFIX, USE_VERSIONS, LIST_TYPE, MIN_AGE));

    public static final Set<Relationship> relationships = Collections.unmodifiableSet(
            new HashSet<>(Collections.singletonList(REL_SUCCESS)));

    public static final String CURRENT_TIMESTAMP = "currentTimestamp";
    public static final String CURRENT_KEY_PREFIX = "key-";

    // State tracking
    private long currentTimestamp = 0L;
    private Set<String> currentKeys;

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

    private void restoreState(final ProcessContext context) throws IOException {
        final StateMap stateMap = context.getStateManager().getState(Scope.CLUSTER);
        if (stateMap.getVersion() == -1L || stateMap.get(CURRENT_TIMESTAMP) == null || stateMap.get(CURRENT_KEY_PREFIX+"0") == null) {
            currentTimestamp = 0L;
            currentKeys = new HashSet<>();
        } else {
            currentTimestamp = Long.parseLong(stateMap.get(CURRENT_TIMESTAMP));
            currentKeys = extractKeys(stateMap);
        }
    }

    private void persistState(final ProcessContext context) {
        Map<String, String> state = new HashMap<>();
        state.put(CURRENT_TIMESTAMP, String.valueOf(currentTimestamp));
        int i = 0;
        for (String key : currentKeys) {
            state.put(CURRENT_KEY_PREFIX+i, key);
            i++;
        }
        try {
            context.getStateManager().setState(state, Scope.CLUSTER);
        } catch (IOException ioe) {
            getLogger().error("Failed to save cluster-wide state. If NiFi is restarted, data duplication may occur", ioe);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        try {
            restoreState(context);
        } catch (IOException ioe) {
            getLogger().error("Failed to restore processor state; yielding", ioe);
            context.yield();
            return;
        }

        final long startNanos = System.nanoTime();
        final String bucket = context.getProperty(BUCKET).evaluateAttributeExpressions().getValue();
        final long minAgeMilliseconds = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final long listingTimestamp = System.currentTimeMillis();

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

        if (delimiter != null && !delimiter.isEmpty()) {
            bucketLister.setDelimiter(delimiter);
        }
        if (prefix != null && !prefix.isEmpty()) {
            bucketLister.setPrefix(prefix);
        }

        VersionListing versionListing;
        final Set<String> listedKeys = new HashSet<>();
        getLogger().trace("Start listing, listingTimestamp={}, currentTimestamp={}, currentKeys={}", new Object[]{listingTimestamp, currentTimestamp, currentKeys});

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

                // Create the attributes
                final Map<String, String> attributes = new HashMap<>();
                attributes.put(CoreAttributes.FILENAME.key(), versionSummary.getKey());
                attributes.put("s3.bucket", versionSummary.getBucketName());
                if (versionSummary.getOwner() != null) { // We may not have permission to read the owner
                    attributes.put("s3.owner", versionSummary.getOwner().getId());
                }
                attributes.put("s3.etag", versionSummary.getETag());
                attributes.put("s3.lastModified", String.valueOf(lastModified));
                attributes.put("s3.length", String.valueOf(versionSummary.getSize()));
                attributes.put("s3.storeClass", versionSummary.getStorageClass());
                attributes.put("s3.isLatest", String.valueOf(versionSummary.isLatest()));
                if (versionSummary.getVersionId() != null) {
                    attributes.put("s3.version", versionSummary.getVersionId());
                }

                if (context.getProperty(WRITE_OBJECT_TAGS).asBoolean()) {
                    attributes.putAll(writeObjectTags(client, versionSummary));
                }

                // Create the flowfile
                FlowFile flowFile = session.create();
                flowFile = session.putAllAttributes(flowFile, attributes);
                session.transfer(flowFile, REL_SUCCESS);

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
            commit(context, session, listCount);
            listCount = 0;
        } while (bucketLister.isTruncated());

        // Update currentKeys.
        if (latestListedTimestampInThisCycle > currentTimestamp) {
            currentKeys.clear();
        }
        currentKeys.addAll(listedKeys);

        // Update stateManger with the most recent timestamp
        currentTimestamp = latestListedTimestampInThisCycle;
        persistState(context);

        final long listMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        getLogger().info("Successfully listed S3 bucket {} in {} millis", new Object[]{bucket, listMillis});

        if (totalListCount == 0) {
            getLogger().debug("No new objects in S3 bucket {} to list. Yielding.", new Object[]{bucket});
            context.yield();
        }
    }

    private boolean commit(final ProcessContext context, final ProcessSession session, int listCount) {
        boolean willCommit = listCount > 0;
        if (willCommit) {
            getLogger().info("Successfully listed {} new files from S3; routing to success", new Object[] {listCount});
            session.commit();
        }
        return willCommit;
    }

    private Map<String, String> writeObjectTags(AmazonS3 client, S3VersionSummary versionSummary) {
        final GetObjectTaggingResult taggingResult = client.getObjectTagging(new GetObjectTaggingRequest(versionSummary.getBucketName(), versionSummary.getKey()));
        final Map<String, String> tagMap = new HashMap<>();

        if (taggingResult != null) {
            final List<Tag> tags = taggingResult.getTagSet();

            for (final Tag tag : tags) {
                tagMap.put("s3.tag." + tag.getKey(), tag.getValue());
            }
        }
        return tagMap;
    }

    private interface S3BucketLister {
        public void setBucketName(String bucketName);
        public void setPrefix(String prefix);
        public void setDelimiter(String delimiter);
        // Versions have a superset of the fields that Objects have, so we'll use
        // them as a common interface
        public VersionListing listVersions();
        public void setNextMarker();
        public boolean isTruncated();
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
}
