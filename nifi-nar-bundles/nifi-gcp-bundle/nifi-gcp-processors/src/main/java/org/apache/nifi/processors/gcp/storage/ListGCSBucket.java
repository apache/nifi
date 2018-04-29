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
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.Stateful;
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
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
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

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.<PropertyDescriptor>builder()
                .addAll(super.getSupportedPropertyDescriptors())
                .add(BUCKET)
                .add(PREFIX)
                .add(USE_GENERATIONS)
                .build();
    }

    public static final Set<Relationship> relationships = Collections.unmodifiableSet(
            new HashSet<>(Collections.singletonList(REL_SUCCESS)));
    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    // State tracking
    public static final String CURRENT_TIMESTAMP = "currentTimestamp";
    public static final String CURRENT_KEY_PREFIX = "key-";
    protected long currentTimestamp = 0L;
    protected Set<String> currentKeys;


    private Set<String> extractKeys(final StateMap stateMap) {
        return stateMap.toMap().entrySet().parallelStream()
                .filter(x -> x.getKey().startsWith(CURRENT_KEY_PREFIX))
                .map(Map.Entry::getValue)
                .collect(Collectors.toSet());
    }

    void restoreState(final ProcessContext context) throws IOException {
        final StateMap stateMap = context.getStateManager().getState(Scope.CLUSTER);
        if (stateMap.getVersion() == -1L || stateMap.get(CURRENT_TIMESTAMP) == null || stateMap.get(CURRENT_KEY_PREFIX+"0") == null) {
            currentTimestamp = 0L;
            currentKeys = new HashSet<>();
        } else {
            currentTimestamp = Long.parseLong(stateMap.get(CURRENT_TIMESTAMP));
            currentKeys = extractKeys(stateMap);
        }
    }

    void persistState(final ProcessContext context) {
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
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        try {
            restoreState(context);
        } catch (IOException e) {
            getLogger().error("Failed to restore processor state; yielding", e);
            context.yield();
            return;
        }

        final long startNanos = System.nanoTime();

        final String bucket = context.getProperty(BUCKET).evaluateAttributeExpressions().getValue();

        final String prefix = context.getProperty(PREFIX).evaluateAttributeExpressions().getValue();

        final boolean useGenerations = context.getProperty(USE_GENERATIONS).asBoolean();

        List<Storage.BlobListOption> listOptions = new ArrayList<>();
        if (prefix != null) {
            listOptions.add(Storage.BlobListOption.prefix(prefix));
        }

        if (useGenerations) {
            listOptions.add(Storage.BlobListOption.versions(true));
        }

        final Storage storage = getCloudService();
        int listCount = 0;
        long maxTimestamp = 0L;


        Page<Blob> blobPages = storage.list(bucket, listOptions.toArray(new Storage.BlobListOption[listOptions.size()]));
        do {
            for (Blob blob : blobPages.getValues()) {
                long lastModified = blob.getUpdateTime();
                if (lastModified < currentTimestamp
                        || lastModified == currentTimestamp && currentKeys.contains(blob.getName())) {
                    continue;
                }

                // Create attributes
                final Map<String, String> attributes = new HashMap<>();

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

                attributes.put(CoreAttributes.FILENAME.key(), blob.getName());

                if (blob.getCreateTime() != null) {
                    attributes.put(CREATE_TIME_ATTR, String.valueOf(blob.getCreateTime()));
                }

                if (blob.getUpdateTime() != null) {
                    attributes.put(UPDATE_TIME_ATTR, String.valueOf(blob.getUpdateTime()));
                }

                // Create the flowfile
                FlowFile flowFile = session.create();
                flowFile = session.putAllAttributes(flowFile, attributes);
                session.transfer(flowFile, REL_SUCCESS);

                // Update state
                if (lastModified > maxTimestamp) {
                    maxTimestamp = lastModified;
                    currentKeys.clear();
                }
                if (lastModified == maxTimestamp) {
                    currentKeys.add(blob.getName());
                }
                listCount++;
            }

            blobPages = blobPages.getNextPage();
            commit(context, session, listCount);
            listCount = 0;
        } while (blobPages != null);

        currentTimestamp = maxTimestamp;

        final long listMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        getLogger().info("Successfully listed GCS bucket {} in {} millis", new Object[]{bucket, listMillis});

        if (!commit(context, session, listCount)) {
            if (currentTimestamp > 0) {
                persistState(context);
            }
            getLogger().debug("No new objects in GCS bucket {} to list. Yielding.", new Object[]{bucket});
            context.yield();
        }
    }

    private boolean commit(final ProcessContext context, final ProcessSession session, int listCount) {
        boolean willCommit = listCount > 0;
        if (willCommit) {
            getLogger().info("Successfully listed {} new files from GCS; routing to success", new Object[] {listCount});
            session.commit();
            persistState(context);
        }
        return willCommit;
    }
}
