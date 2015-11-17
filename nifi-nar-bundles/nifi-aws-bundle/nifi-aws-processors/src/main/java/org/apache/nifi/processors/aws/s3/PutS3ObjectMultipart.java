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

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.BufferedInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@SeeAlso({FetchS3Object.class, PutS3Object.class, DeleteS3Object.class})
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"Amazon", "S3", "AWS", "Archive", "Put", "Multi", "Multipart", "Upload"})
@CapabilityDescription("Puts FlowFiles to an Amazon S3 Bucket using the MultipartUpload API method.  " +
        "This upload consists of three steps 1) initiate upload, 2) upload the parts, and 3) complete the upload.\n" +
        "Since the intent for this processor involves large files, the processor saves state locally after each step " +
        "so that an upload can be resumed without having to restart from the beginning of the file.\n" +
        "The AWS libraries default to using standard AWS regions but the 'Endpoint Override URL' allows this to be " +
        "overridden.")
@DynamicProperty(name = "The name of a User-Defined Metadata field to add to the S3 Object",
        value = "The value of a User-Defined Metadata field to add to the S3 Object",
        description = "Allows user-defined metadata to be added to the S3 object as key/value pairs",
        supportsExpressionLanguage = true)
@ReadsAttribute(attribute = "filename", description = "Uses the FlowFile's filename as the filename for the S3 object")
@WritesAttributes({
        @WritesAttribute(attribute = "s3.bucket", description = "The S3 bucket where the Object was put in S3"),
        @WritesAttribute(attribute = "s3.key", description = "The S3 key within where the Object was put in S3"),
        @WritesAttribute(attribute = "s3.version", description = "The version of the S3 Object that was put to S3"),
        @WritesAttribute(attribute = "s3.etag", description = "The ETag of the S3 Object"),
        @WritesAttribute(attribute = "s3.uploadId", description = "The uploadId used to upload the Object to S3"),
        @WritesAttribute(attribute = "s3.expiration", description = "A human-readable form of the expiration date of " +
                "the S3 object, if one is set"),
        @WritesAttribute(attribute = "s3.usermetadata", description = "A human-readable form of the User Metadata " +
                "of the S3 object, if any was set")
})
public class PutS3ObjectMultipart extends AbstractS3Processor {

    public static final long MIN_BYTES_INCLUSIVE = 50L * 1024L * 1024L;
    public static final long MAX_BYTES_INCLUSIVE = 5L * 1024L * 1024L * 1024L;
    public static final String PERSISTENCE_ROOT = "conf/state/";

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

    public static final PropertyDescriptor PART_SIZE = new PropertyDescriptor.Builder()
            .name("Part Size")
            .description("Specifies the Part Size to be used for the S3 Multipart Upload API.  The flow file will be " +
                    "broken into Part Size chunks during upload.  Part size must be at least 50MB and no more than " +
                    "5GB, but the final part can be less than 50MB.")
            .required(true)
            .defaultValue("5 GB")
            .addValidator(StandardValidators.createDataSizeBoundsValidator(MIN_BYTES_INCLUSIVE, MAX_BYTES_INCLUSIVE))
            .build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(KEY, BUCKET, PART_SIZE, ENDPOINT_OVERRIDE, ACCESS_KEY, SECRET_KEY, CREDENTIALS_FILE,
                    SSL_CONTEXT_SERVICE, STORAGE_CLASS, REGION, TIMEOUT, EXPIRATION_RULE_ID,
                    FULL_CONTROL_USER_LIST, READ_USER_LIST, WRITE_USER_LIST, READ_ACL_LIST, WRITE_ACL_LIST, OWNER));

    final static String S3_BUCKET_KEY = "s3.bucket";
    final static String S3_OBJECT_KEY = "s3.key";
    final static String S3_UPLOAD_ID_ATTR_KEY = "s3.uploadId";
    final static String S3_VERSION_ATTR_KEY = "s3.version";
    final static String S3_ETAG_ATTR_KEY = "s3.etag";
    final static String S3_EXPIRATION_ATTR_KEY = "s3.expiration";
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

    protected File getPersistenceFile() {
        return new File(PERSISTENCE_ROOT + getIdentifier());
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if ( descriptor.equals(KEY)
                || descriptor.equals(BUCKET)
                || descriptor.equals(ENDPOINT_OVERRIDE)
                || descriptor.equals(STORAGE_CLASS)
                || descriptor.equals(REGION)) {
            destroyState();
        }
    }

    protected MultipartState getState(final String s3ObjectKey) throws IOException {
        // get local state if it exists
        MultipartState currState = null;
        final File persistenceFile = getPersistenceFile();
        if (persistenceFile.exists()) {
            try (final FileInputStream fis = new FileInputStream(persistenceFile)) {
                final Properties props = new Properties();
                props.load(fis);
                if (props.containsKey(s3ObjectKey)) {
                    final String localSerialState = props.getProperty(s3ObjectKey);
                    if (localSerialState != null) {
                        currState = new MultipartState(localSerialState);
                        getLogger().info("Local state for {} loaded with uploadId {} and {} partETags",
                                new Object[]{s3ObjectKey, currState.uploadId, currState.partETags.size()});
                    }
                }
            } catch (IOException ioe) {
                getLogger().warn("Failed to recover local state for {} due to {}. Assuming no local state and " +
                                "restarting upload.", new Object[]{s3ObjectKey, ioe.getMessage()});
            }
        }
        return currState;
    }

    protected void persistState(final String s3ObjectKey, final MultipartState currState) throws IOException {
        final String currStateStr = (currState == null) ? null : currState.toString();
        final File persistenceFile = getPersistenceFile();
        final File parentDir = persistenceFile.getParentFile();
        if (!parentDir.exists() && !parentDir.mkdirs()) {
            throw new IOException("Could not create persistence directory " + parentDir.getAbsolutePath() +
                " needed to store local state.");
        }
        final Properties props = new Properties();
        if (persistenceFile.exists()) {
            try (final FileInputStream fis = new FileInputStream(persistenceFile)) {
                props.load(fis);
            }
        }
        if (currStateStr != null) {
            props.setProperty(s3ObjectKey, currStateStr);
        } else {
            props.remove(s3ObjectKey);
        }

        try (final FileOutputStream fos = new FileOutputStream(persistenceFile)) {
            props.store(fos, null);
        } catch (IOException ioe) {
            getLogger().error("Could not store state {} due to {}.",
                    new Object[]{persistenceFile.getAbsolutePath(), ioe.getMessage()});
        }
    }

    protected void removeState(final String s3ObjectKey) throws IOException {
        persistState(s3ObjectKey, null);
    }

    protected void destroyState() {
        final File persistenceFile = getPersistenceFile();
        if (persistenceFile.exists()) {
            if (!persistenceFile.delete()) {
                getLogger().warn("Could not delete state file {}, attempting to delete contents.",
                        new Object[]{persistenceFile.getAbsolutePath()});
            } else {
                try (final FileOutputStream fos = new FileOutputStream(persistenceFile)) {
                    new Properties().store(fos, null);
                } catch (IOException ioe) {
                    getLogger().error("Could not store empty state file {} due to {}.",
                            new Object[]{persistenceFile.getAbsolutePath(), ioe.getMessage()});
                }
            }
        }
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
        final String cacheKey = getIdentifier() + "/" + bucket + "/" + key;

        final AmazonS3 s3 = getClient();
        final FlowFile ff = flowFile;
        final String ffFilename = ff.getAttributes().get(CoreAttributes.FILENAME.key());
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
                        final String expirationRule = context.getProperty(EXPIRATION_RULE_ID)
                                .evaluateAttributeExpressions(ff).getValue();
                        if (expirationRule != null) {
                            objectMetadata.setExpirationTimeRuleId(expirationRule);
                        }
                        final Map<String, String> userMetadata = new HashMap<>();
                        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
                            if (entry.getKey().isDynamic()) {
                                final String value = context.getProperty(entry.getKey())
                                        .evaluateAttributeExpressions(ff).getValue();
                                userMetadata.put(entry.getKey().getName(), value);
                            }
                        }
                        if (!userMetadata.isEmpty()) {
                            objectMetadata.setUserMetadata(userMetadata);
                        }

                        // load or create persistent state
                        //------------------------------------------------------------
                        MultipartState currentState;
                        try {
                            currentState = getState(cacheKey);
                            if (currentState != null) {
                                if (currentState.partETags.size() > 0) {
                                    final PartETag lastETag = currentState.partETags.get(currentState.partETags.size() - 1);
                                    getLogger().info("RESUMING UPLOAD for flowfile='{}' bucket='{}' key='{}' " +
                                                    "uploadID='{}' filePosition='{}' partSize='{}' storageClass='{}' " +
                                                    "contentLength='{}' partsLoaded={} lastPart={}/{}",
                                            new Object[]{ffFilename, bucket, key, currentState.uploadId,
                                                    currentState.filePosition, currentState.partSize,
                                                    currentState.storageClass.toString(), currentState.contentLength,
                                                    currentState.partETags.size(),
                                                    Integer.toString(lastETag.getPartNumber()), lastETag.getETag()});
                                } else {
                                    getLogger().info("RESUMING UPLOAD for flowfile='{}' bucket='{}' key='{}' " +
                                                    "uploadID='{}' filePosition='{}' partSize='{}' storageClass='{}' " +
                                                    "contentLength='{}' no partsLoaded",
                                            new Object[]{ffFilename, bucket, key, currentState.uploadId,
                                                    currentState.filePosition, currentState.partSize,
                                                    currentState.storageClass.toString(), currentState.contentLength});
                                }
                            } else {
                                currentState = new MultipartState();
                                currentState.setPartSize(context.getProperty(PART_SIZE).asDataSize(DataUnit.B).longValue());
                                currentState.setStorageClass(StorageClass.valueOf(context.getProperty(STORAGE_CLASS).getValue()));
                                currentState.setContentLength(ff.getSize());
                                persistState(cacheKey, currentState);
                                getLogger().info("STARTING NEW UPLOAD for flowfile='{}' bucket='{}' key='{}'",
                                        new Object[]{ffFilename, bucket, key});
                            }
                        } catch (IOException e) {
                            getLogger().error("Processing flow files, IOException initiating cache state: " + e.getMessage());
                            throw(e);
                        }

                        // initiate upload or find position in file
                        //------------------------------------------------------------
                        if (currentState.uploadId.isEmpty()) {
                            final InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest(bucket, key, objectMetadata);
                            initiateRequest.setStorageClass(currentState.storageClass);
                            final AccessControlList acl = createACL(context, ff);
                            if (acl != null) {
                                initiateRequest.setAccessControlList(acl);
                            }
                            try {
                                final InitiateMultipartUploadResult initiateResult = s3.initiateMultipartUpload(initiateRequest);
                                currentState.setUploadId(initiateResult.getUploadId());
                                currentState.partETags.clear();
                                currentState.uploadETag = "";
                                try {
                                    persistState(cacheKey, currentState);
                                } catch (Exception e) {
                                    getLogger().info("Processing flow file, Exception saving cache state: " + e.getMessage());
                                }
                                getLogger().info("SUCCESS initiate upload flowfile={} available={} position={} " +
                                        "length={} bucket={} key={} uploadId={}", new Object[]{ffFilename, in.available(),
                                        currentState.filePosition, currentState.contentLength, bucket, key, currentState.uploadId});
                                if (initiateResult.getUploadId() != null) {
                                    attributes.put(S3_UPLOAD_ID_ATTR_KEY, initiateResult.getUploadId());
                                }
                            } catch (AmazonClientException e) {
                                getLogger().info("FAILURE initiate upload flowfile={} bucket={} key={} reason={}",
                                        new Object[]{ffFilename, bucket, key, e.getMessage()});
                            }
                        } else {
                            if (currentState.filePosition > 0) {
                                try {
                                    final long skipped = in.skip(currentState.filePosition);
                                    if (skipped != currentState.filePosition) {
                                        getLogger().info("FAILURE skipping to resume upload flowfile={} bucket={} key={} position={} skipped={}",
                                                new Object[]{ffFilename, bucket, key, currentState.filePosition, skipped});
                                    }
                                } catch (Exception e) {
                                    getLogger().info("FAILURE skipping to resume upload flowfile={} bucket={} key={} position={} reason={}",
                                            new Object[]{ffFilename, bucket, key, currentState.filePosition, e.getMessage()});
                                }
                            }
                        }

                        // upload parts
                        //------------------------------------------------------------
                        long thisPartSize;
                        for (int part = currentState.partETags.size() + 1;
                             currentState.filePosition < currentState.contentLength; part++) {
                            if (!PutS3ObjectMultipart.this.isScheduled()) {
                                getLogger().info("PARTSIZE stopping download, processor unscheduled flowfile={} part={} uploadId={}",
                                        new Object[]{ ffFilename, part, currentState.uploadId });
                                session.rollback();
                                return;
                            }
                            thisPartSize = Math.min(currentState.partSize, (currentState.contentLength - currentState.filePosition));
                            UploadPartRequest uploadRequest = new UploadPartRequest()
                                    .withBucketName(bucket)
                                    .withKey(key)
                                    .withUploadId(currentState.uploadId)
                                    .withInputStream(in)
                                    .withPartNumber(part)
                                    .withPartSize(thisPartSize);
                            try {
                                UploadPartResult uploadPartResult = s3.uploadPart(uploadRequest);
                                currentState.addPartETag(uploadPartResult.getPartETag());
                                currentState.filePosition += thisPartSize;
                                try {
                                    persistState(cacheKey, currentState);
                                } catch (Exception e) {
                                    getLogger().info("Processing flow file, Exception saving cache state: " + e.getMessage());
                                }
                                getLogger().info("SUCCESS upload flowfile={} part={} available={} etag={} uploadId={}",
                                        new Object[]{ffFilename, part, in.available(), uploadPartResult.getETag(),
                                                currentState.uploadId});
                            } catch (AmazonClientException e) {
                                getLogger().info("FAILURE upload flowfile={} part={} bucket={} key={} reason={}",
                                        new Object[]{ffFilename, part, bucket, key, e.getMessage()});
                                e.printStackTrace();
                            }
                        }

                        // complete upload
                        //------------------------------------------------------------
                        CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(
                                bucket, key, currentState.uploadId, currentState.partETags);
                        try {
                            CompleteMultipartUploadResult completeResult = s3.completeMultipartUpload(completeRequest);
                            getLogger().info("SUCCESS complete upload flowfile={} etag={} uploadId={}",
                                    new Object[]{ffFilename, completeResult.getETag(), currentState.uploadId});
                            currentState.setUploadETag(completeResult.getETag());
                            if (completeResult.getVersionId() != null) {
                                attributes.put(S3_VERSION_ATTR_KEY, completeResult.getVersionId());
                            }
                            if (completeResult.getETag() != null) {
                                attributes.put(S3_ETAG_ATTR_KEY, completeResult.getETag());
                            }
                            if (completeResult.getExpirationTime() != null) {
                                attributes.put(S3_EXPIRATION_ATTR_KEY, completeResult.getExpirationTime().toString());
                            }
                            if (userMetadata.size() > 0) {
                                StringBuilder userMetaBldr = new StringBuilder();
                                for (String userKey : userMetadata.keySet()) {
                                    userMetaBldr.append(userKey).append("=").append(userMetadata.get(userKey));
                                }
                                attributes.put(S3_USERMETA_ATTR_KEY, userMetaBldr.toString());
                            }
                        } catch (AmazonClientException e) {
                            getLogger().info("FAILURE complete upload flowfile={} bucket={} key={} reason={}",
                                    new Object[]{ffFilename, bucket, key, e.getMessage()});
                            e.printStackTrace();
                        }
                    }
                }
            });

            if (!attributes.isEmpty()) {
                flowFile = session.putAllAttributes(flowFile, attributes);
            }

            final String url = ((AmazonS3Client)s3).getResourceUrl(bucket, key);
            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().send(flowFile, url, millis);
            getLogger().info("Successfully put {} to Amazon S3 in {} milliseconds", new Object[]{ff, millis});

            session.transfer(flowFile, REL_SUCCESS);
            try {
                removeState(cacheKey);
            } catch (IOException e) {
                getLogger().info("Error trying to delete from cache: " + e.getMessage());
            }
        } catch (final ProcessException | AmazonClientException pe) {
            getLogger().error("Failed to put {} to Amazon S3 due to {}", new Object[]{flowFile, pe});
            getLogger().error(pe.getMessage());
            session.transfer(flowFile, REL_FAILURE);
            try {
                removeState(cacheKey);
            } catch (IOException e) {
                getLogger().info("Error trying to delete key {} from cache: {}",
                        new Object[]{cacheKey, e.getMessage()});
            }
        }
    }

    public static class MultipartState implements Serializable {

        public static final String SEPARATOR = "#";

        public String uploadId;
        public Long filePosition;
        public List<PartETag> partETags;
        public String uploadETag;
        public Long partSize;
        public StorageClass storageClass;
        public Long contentLength;

        public MultipartState() {
            uploadId = "";
            filePosition = 0L;
            partETags = new ArrayList<>();
            uploadETag = "";
            partSize = 0L;
            storageClass = StorageClass.Standard;
            contentLength = 0L;
        }

        // create from a previous toString() result
        public MultipartState(String buf) {
            String[] fields = buf.split(SEPARATOR);
            uploadId = fields[0];
            filePosition = Long.parseLong(fields[1]);
            partETags = new ArrayList<>();
            for (String part : fields[2].split(",")) {
                if (part != null && !part.isEmpty()) {
                    String[] partFields = part.split("/");
                    partETags.add(new PartETag(Integer.parseInt(partFields[0]), partFields[1]));
                }
            }
            uploadETag = fields[3];
            partSize = Long.parseLong(fields[4]);
            storageClass = StorageClass.fromValue(fields[5]);
            contentLength = Long.parseLong(fields[6]);
        }

        public void setUploadId(String id) {
            uploadId = id;
        }
        public void setFilePosition(Long pos) {
            filePosition = pos;
        }
        public void addPartETag(PartETag tag) {
            partETags.add(tag);
        }
        public void setUploadETag(String tag) {
            uploadETag = tag;
        }
        public void setPartSize(Long size) {
            partSize = size;
        }
        public void setStorageClass(StorageClass aClass) {
            storageClass = aClass;
        }
        public void setContentLength(Long length) {
            contentLength = length;
        }

        public String toString() {
            StringBuilder buf = new StringBuilder();
            buf.append(uploadId).append(SEPARATOR)
                    .append(filePosition.toString()).append(SEPARATOR);
            if (partETags.size() > 0) {
                boolean first = true;
                for (PartETag tag : partETags) {
                    if (!first) {
                        buf.append(",");
                    } else {
                        first = false;
                    }
                    buf.append(String.format("%d/%s", tag.getPartNumber(), tag.getETag()));
                }
            }
            buf.append(SEPARATOR)
                .append(uploadETag).append(SEPARATOR)
                .append(partSize.toString()).append(SEPARATOR)
                .append(storageClass.toString()).append(SEPARATOR)
                .append(contentLength.toString());
            return buf.toString();
        }
    }
}
