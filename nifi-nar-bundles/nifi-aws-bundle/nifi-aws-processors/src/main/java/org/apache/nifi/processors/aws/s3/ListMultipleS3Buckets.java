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

import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.apache.commons.lang3.StringUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processors.aws.s3.ListS3.DELIMITER;
import static org.apache.nifi.processors.aws.s3.ListS3.PREFIX;
import static org.apache.nifi.processors.aws.s3.ListS3.USE_VERSIONS;
import static org.apache.nifi.processors.aws.s3.ListS3.LIST_TYPE;
import static org.apache.nifi.processors.aws.s3.ListS3.MIN_AGE;
import static org.apache.nifi.processors.aws.s3.ListS3.WRITE_OBJECT_TAGS;

import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
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
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;

import com.amazonaws.services.s3.AmazonS3;

@PrimaryNodeOnly
@TriggerSerially
@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"Amazon", "S3", "AWS", "list", "watch", "buckets"})
@CapabilityDescription("Watches objects in one or more S3 buckets based on the 'Bucket' property, which supports Expression Language with FlowFile Attributes. "
        + " For each found object, creates a output FlowFile that represents the object so that it can be fetched in conjunction with FetchS3Object. "
        + "This Processor is designed to run on Primary Node only in a cluster. "
        + "If the primary node changes, the new Primary Node will pick up where the previous node left off without duplicating all of the data.")
@Stateful(scopes = Scope.CLUSTER, description = "After retrieving a listing of S3 Objects(keys), the timestamp of the newest key is stored, "
        + "along with a bucket-specific (object) key that share that same timestamp. "
        + "This allows the Processor to list only keys that have been added or modified after this date the next time that the Processor is run. "
        + "State is stored across the cluster so that this Processor can be run on Primary Node only and if a new Primary "
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
public class ListMultipleS3Buckets extends AbstractS3Processor {

    public static final PropertyDescriptor REQUESTER_PAYS = new PropertyDescriptor.Builder()
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

    public static final PropertyDescriptor WRITE_USER_METADATA = new PropertyDescriptor.Builder()
            .name("write-s3-user-metadata")
            .displayName("Write User Metadata")
            .description("If set to 'True', the user defined metadata associated with the S3 object will be written as FlowFile attributes")
            .required(true)
            .allowableValues(new AllowableValue("true", "True"), new AllowableValue("false", "False"))
            .defaultValue("false")
            .build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(BUCKET, REGION, ACCESS_KEY, SECRET_KEY, WRITE_OBJECT_TAGS, WRITE_USER_METADATA, CREDENTIALS_FILE,
                    AWS_CREDENTIALS_PROVIDER_SERVICE, TIMEOUT, SSL_CONTEXT_SERVICE, ENDPOINT_OVERRIDE,
                    SIGNER_OVERRIDE, PROXY_CONFIGURATION_SERVICE, PROXY_HOST, PROXY_HOST_PORT, PROXY_USERNAME,
                    PROXY_PASSWORD, DELIMITER, PREFIX, USE_VERSIONS, LIST_TYPE, MIN_AGE, REQUESTER_PAYS));

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("For each S3 File Object found, 1 FlowFile will be routed to this destination")
            .build();
    private static final String ERR_MSG_BUCKET_DOES_NOT_EXIST = "specified bucket does not exist";

    public static Set<Relationship> relationships =
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
                    REL_SUCCESS)));

    public static final String CURRENT_TIMESTAMP = "currentTimestamp";
    protected static final String CURRENT_KEY_PREFIX = "key-";
    //used as a separator character in the composite "keys" used to track state of each object
    protected static final String STATE_MGMT_KEY_DELIMITER = ":";
    protected static final String COUNT = "count";

    // State Management Logic tracking
    private long currentTimestamp = 0L;
    private Set<String> currentKeys;

    protected static Validator createRequesterPaysValidator() {
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


    private void restoreState(final ProcessContext context, final String bucketName) throws RuntimeException {
        final StateMap stateMap;
        try {
            stateMap = context.getStateManager().getState(Scope.CLUSTER);
        } catch (IOException ioe) {
            getLogger().error("Err: Restore State - Not Able to retrieve the existing State Map", ioe);
            throw new RuntimeException(ioe);
        }

        String timestampKey = getTimestampKey(bucketName);
        String firstObjKey = getObjKeysPrefix(bucketName) + "0";
        String objectCountStr = stateMap.get(getBucketCountStateMapKey(bucketName));
        if (notContainsEntriesForBucket(stateMap, timestampKey, firstObjKey,
                objectCountStr)) {
            currentTimestamp = 0L;
            currentKeys = new HashSet<>();
        } else {
            currentTimestamp = Long.parseLong(stateMap.get(timestampKey));
            //NOTE: restore State ONLY for this Bucket by using bucketName in obj-key prefix
            currentKeys = extractKeysForBucket(stateMap, bucketName, Integer.valueOf(objectCountStr));
        }
    }

    private Set<String> extractKeysForBucket(final StateMap stateMap,
                                             final String bucketName,
                                             final Integer objectCount) {

        Set<String> keys = new HashSet<>();
        Map<String, String> stateMapEntries = stateMap.toMap();
        String keyPrefix = getObjKeysPrefix(bucketName);
        for (Integer i = 0; i < objectCount; i++) {
            String objectKey = keyPrefix + i;
            keys.add(stateMapEntries.get(objectKey));
        }

        return keys;
    }

    private void persistState(final ProcessContext context, String bucketName) throws RuntimeException {
        getLogger().info("Updating Persisted State for bucket {} ", new Object[]{bucketName});
        final StateMap stateMap;
        try {
            stateMap = context.getStateManager().getState(Scope.CLUSTER);
        } catch (IOException ioe) {
            getLogger().error("Err: Not Able to retrieve the existing State Map", ioe);
            throw new RuntimeException(ioe);
        }

        Map<String, String> newState;
        if (stateMap.getVersion() == -1) {
            newState = new HashMap<>();
        } else {
            newState = removeThisBucketStateEntries(stateMap.toMap(), bucketName);
        }
        addStateMapEntriesForThisBucket(bucketName, newState);
        try {
            context.getStateManager().setState(newState, Scope.CLUSTER);
        } catch (IOException ioe) {
            getLogger().error("Uh-oh, Failed to save cluster-wide state. If NiFi is restarted, data duplication may occur", ioe);
            throw new RuntimeException(ioe);
        }
    }

    private Map<String, String> removeThisBucketStateEntries(Map<String, String> existingState, String bucketName) {

        //Check that this Bucket has some Existing entries, else return
        String bucketCountKey = getBucketCountStateMapKey(bucketName);
        String timestampKey = getTimestampKey(bucketName);
        if (existingState.get(timestampKey) == null
                || existingState.get(bucketCountKey) == null) {
            return new HashMap<>(existingState);
        }

        getLogger().debug("Removing Outdated StateMap entries for bucket {} ", new Object[]{bucketName});
        String objectCountStr = existingState.get(bucketCountKey);
        if (StringUtils.isNumeric(objectCountStr) == false){
            throw new RuntimeException("Err(RemoveBucketEntries): This BucketCount value is invalid: not Numeric: " + objectCountStr);
        }
        Integer objectCount = Integer.valueOf(objectCountStr);
        Map<String, String> updatedState = new HashMap<>(existingState);

        for (Integer i = 0; i < objectCount; i++) {
            String oneObjectKey = getObjKeysPrefix(bucketName) + i;
            updatedState.remove(oneObjectKey);
        }
        updatedState.remove(bucketCountKey);

        updatedState.remove(timestampKey);
        return updatedState;
    }

    private void addStateMapEntriesForThisBucket(String bucketName, Map<String, String> state) {
        getLogger().debug("Adding New/Updated StateMap entries for bucket {} ", new Object[]{bucketName});

        //Add Bucket-Timestamp entry to StateMap  //example: bucket1:currentTimestamp
        String timestampKey = getTimestampKey(bucketName);
        state.put(timestampKey, String.valueOf(currentTimestamp));

        //Add "count" entry for this bucket -> a count of how many "Object Key" State Map entries for this bucket
        int count = currentKeys.size();
        String bucketCountKey = getBucketCountStateMapKey(bucketName);
        state.put(bucketCountKey, String.valueOf(count));

        //Add each "Obj key" (S3 filename) for this Bucket from the S3 List request to the StateMap
        int i = 0;
        for (String s3ObjKey : currentKeys) {
            String stateMapKey = getObjKeysPrefix(bucketName) + i;
            state.put(stateMapKey, s3ObjKey);
            i++;
        }
    }

    private static String getTimestampKey(String bucketName) {
        return bucketName + STATE_MGMT_KEY_DELIMITER + CURRENT_TIMESTAMP;
    }

    private String getBucketCountStateMapKey(String bucketName) {
        return bucketName + STATE_MGMT_KEY_DELIMITER + COUNT;
    }

    private String getObjKeysPrefix(String bucketName) {
        return bucketName + STATE_MGMT_KEY_DELIMITER + CURRENT_KEY_PREFIX;
    }

    private boolean notContainsEntriesForBucket(StateMap stateMap,
                                                String timestampKey,
                                                String firstObjKey, String objectCountStr) {
        return stateMap.getVersion() == -1L
                || stateMap.get(timestampKey) == null
                || stateMap.get(firstObjKey) == null
                || StringUtils.isEmpty(objectCountStr)
                || StringUtils.isNumeric(objectCountStr) == false;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile inputFlowFile = session.get();

        if(inputFlowFile == null){
            return;
        }

        final String bucket = context.getProperty(BUCKET).evaluateAttributeExpressions(inputFlowFile).getValue();

        try {
            restoreState(context, bucket);
        } catch (RuntimeException re) {
            throw re;
        }

        final long minAgeMilliseconds = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final long listingTimestamp = System.currentTimeMillis();
        final boolean requesterPays = context.getProperty(REQUESTER_PAYS).asBoolean();
        long latestListedTimestampInThisCycle = currentTimestamp;
        String delimiter = context.getProperty(DELIMITER).getValue();
        String prefix = context.getProperty(PREFIX).evaluateAttributeExpressions().getValue();
        boolean useVersions = context.getProperty(USE_VERSIONS).asBoolean();
        int listType = context.getProperty(LIST_TYPE).asInteger();
        final long startNanos = System.nanoTime();

        final AmazonS3 client = getClient();
        S3BucketLister bucketLister = createS3BucketLister(bucket, requesterPays,
                delimiter, prefix, useVersions, listType, client);

        VersionListing versionListing;
        List<FlowFile> outputFlowfiles = new ArrayList<>();
        final Set<String> listedKeys = new HashSet<>();
        getLogger().info("Start Watch for S3 Bucket, bucketName ={}, listingTimestamp={}, " +
                        "currentTimestamp={}, currentKeysCount={}",
                new Object[]{bucket, listingTimestamp, currentTimestamp, currentKeys.size()});

        do {
            try {
                versionListing = bucketLister.listVersions();
                for (S3VersionSummary versionSummary : versionListing.getVersionSummaries()) {
                    long lastModified = versionSummary.getLastModified().getTime();
                    if (objectHasNotBeenUpdated(versionSummary, lastModified)
                            || objectOlderThanMinAgeProperty(minAgeMilliseconds, listingTimestamp, lastModified)) {
                        getLogger().debug("S3 Object={} is Not New or Not min Age (in seconds)={}",
                                new Object[]{versionSummary.getKey(), minAgeMilliseconds * 1000});
                        continue;
                    }

                    getLogger().debug("Listed key={}, lastModified={}, currentKeys={}",
                            new Object[]{versionSummary.getKey(), lastModified, currentKeys});

                    // Create the attributes
                    final Map<String, String> attributes =
                            createOutputFlowfileAttributes(context, client,
                                    versionSummary, lastModified);

                    FlowFile outputFlowFile = session.create(inputFlowFile);
                    session.putAllAttributes(outputFlowFile, attributes);
                    outputFlowfiles.add(outputFlowFile);

                    // Track the latest lastModified timestamp and keys having that timestamp.
                    // NOTE: Amazon S3 lists objects in UTF-8 character encoding in lexicographical order. Not ordered by timestamps.
                    if (lastModified > latestListedTimestampInThisCycle) {
                        latestListedTimestampInThisCycle = lastModified;
                        listedKeys.clear();
                        listedKeys.add(versionSummary.getKey());

                    } else if (lastModified == latestListedTimestampInThisCycle) {
                        listedKeys.add(versionSummary.getKey());
                    }

                }
                bucketLister.setNextMarker();
            } catch (Exception e) {
                //If Bucket Not Found AmazonS3Exception -> Log details & yield
                boolean noBucketFound = handleBucketNotFoundException(e, bucket, context);
                if (noBucketFound) {
                    session.remove(inputFlowFile);
                    session.commit();
                    return;
                } else {  //DO NOT swallow other types of exceptions.
                    throw e;
                }
            }
        } while (bucketLister.isTruncated());

        // Update currentKeys
        if (latestListedTimestampInThisCycle > currentTimestamp) {
            currentKeys.clear();
        }
        currentKeys.addAll(listedKeys);

        // Update stateManger with the most recent timestamp
        currentTimestamp = latestListedTimestampInThisCycle;
        persistState(context, bucket);

        session.transfer(outputFlowfiles, REL_SUCCESS);
        session.remove(inputFlowFile);
        commit(session, outputFlowfiles.size());

        final long listMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        getLogger().info("Successfully listed S3 bucket {} in {} millis", new Object[]{bucket, listMillis});

        if (outputFlowfiles.size() == 0) {
            getLogger().debug("No new objects in S3 bucket {} to list. Yielding.", new Object[]{bucket});
            context.yield();
        }
    }

    private boolean handleBucketNotFoundException(Exception e, String bucket, ProcessContext context) {
        if(e instanceof AmazonS3Exception ) {
            AmazonS3Exception s3Exception = (AmazonS3Exception) e;
            if (s3Exception.getLocalizedMessage().contains(ERR_MSG_BUCKET_DOES_NOT_EXIST)) {
                getLogger().warn("S3 bucket '{}' does Not exist. ",
                        new Object[]{bucket});
                getLogger().debug("AmazonS3Exception msg: " + s3Exception.getLocalizedMessage());
                getLogger().info("No objects for 'Not Found' S3 bucket {} to list. Yielding.", new Object[]{bucket});
                return true;
            }
        }
        return false;
    }

    private boolean objectOlderThanMinAgeProperty(long minAgeMilliseconds,
                                                  long listingTimestamp, long lastModified) {
        return lastModified > (listingTimestamp - minAgeMilliseconds);
    }

    private S3BucketLister createS3BucketLister(String bucket, boolean requesterPays,
                                                String delimiter, String prefix,
                                                boolean useVersions, int listType, AmazonS3 client) {

        S3BucketLister bucketLister = getS3BucketLister(client, useVersions, listType);
        bucketLister.setBucketName(bucket);
        bucketLister.setRequesterPays(requesterPays);
        if (delimiter != null && !delimiter.isEmpty()) {
            bucketLister.setDelimiter(delimiter);
        }
        if (prefix != null && !prefix.isEmpty()) {
            bucketLister.setPrefix(prefix);
        }
        return bucketLister;
    }

    /**
     *  Create the Map of attributes for one S3 Object -> associated with one
     *  (Success Rel) Output FlowFile.
     *
     * @param context The ProcessContext - used for property retrieval here
     * @param client S3 Client
     * @param versionSummary - S3VersionInfo
     * @param lastModified time of last S3 bucket update
     * @return
     */
    private Map<String, String> createOutputFlowfileAttributes(ProcessContext context,
                                                               AmazonS3 client,
                                                               S3VersionSummary versionSummary, long lastModified) {

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
        if (context.getProperty(WRITE_USER_METADATA).asBoolean()) {
            attributes.putAll(writeUserMetadata(client, versionSummary));
        }
        return attributes;
    }

    private void commit(final ProcessSession session, int objCount) {
        if (objCount > 0) {
            session.commit();
        }
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

    private Map<String, String> writeUserMetadata(AmazonS3 client, S3VersionSummary versionSummary) {
        ObjectMetadata objectMetadata = client.getObjectMetadata(new GetObjectMetadataRequest(versionSummary.getBucketName(), versionSummary.getKey()));
        final Map<String, String> metadata = new HashMap<>();
        if (objectMetadata != null) {
            for (Map.Entry<String, String> e : objectMetadata.getUserMetadata().entrySet()) {
                metadata.put("s3.user.metadata." + e.getKey(), e.getValue());
            }
        }
        return metadata;
    }

    private S3BucketLister getS3BucketLister(AmazonS3 client, boolean useVersions, int listType) {
        return useVersions
                ? new S3VersionBucketLister(client)
                : listType == 2
                ? new S3ObjectBucketListerVersion2(client)
                : new S3ObjectBucketLister(client);
    }

    private boolean objectHasNotBeenUpdated(S3VersionSummary versionSummary, long lastModified) {
        return lastModified < currentTimestamp
                || (lastModified == currentTimestamp && currentKeys.contains(versionSummary.getKey()));
    }

}
