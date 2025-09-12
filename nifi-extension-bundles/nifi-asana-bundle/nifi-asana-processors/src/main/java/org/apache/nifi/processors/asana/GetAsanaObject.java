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
package org.apache.nifi.processors.asana;

import org.apache.http.entity.ContentType;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.controller.asana.AsanaClient;
import org.apache.nifi.controller.asana.AsanaClientProviderService;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.asana.utils.AsanaObject;
import org.apache.nifi.processors.asana.utils.AsanaObjectFetcher;
import org.apache.nifi.processors.asana.utils.AsanaObjectState;
import org.apache.nifi.processors.asana.utils.AsanaProjectEventFetcher;
import org.apache.nifi.processors.asana.utils.AsanaProjectFetcher;
import org.apache.nifi.processors.asana.utils.AsanaProjectMembershipFetcher;
import org.apache.nifi.processors.asana.utils.AsanaProjectStatusAttachmentFetcher;
import org.apache.nifi.processors.asana.utils.AsanaProjectStatusFetcher;
import org.apache.nifi.processors.asana.utils.AsanaStoryFetcher;
import org.apache.nifi.processors.asana.utils.AsanaTagFetcher;
import org.apache.nifi.processors.asana.utils.AsanaTaskAttachmentFetcher;
import org.apache.nifi.processors.asana.utils.AsanaTaskFetcher;
import org.apache.nifi.processors.asana.utils.AsanaTeamFetcher;
import org.apache.nifi.processors.asana.utils.AsanaTeamMemberFetcher;
import org.apache.nifi.processors.asana.utils.AsanaUserFetcher;
import org.apache.nifi.reporting.InitializationException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Collections.singletonMap;
import static org.apache.nifi.processors.asana.AsanaObjectType.AV_COLLECT_PROJECT_EVENTS;
import static org.apache.nifi.processors.asana.AsanaObjectType.AV_COLLECT_PROJECT_MEMBERS;
import static org.apache.nifi.processors.asana.AsanaObjectType.AV_COLLECT_PROJECT_STATUS_ATTACHMENTS;
import static org.apache.nifi.processors.asana.AsanaObjectType.AV_COLLECT_PROJECT_STATUS_UPDATES;
import static org.apache.nifi.processors.asana.AsanaObjectType.AV_COLLECT_STORIES;
import static org.apache.nifi.processors.asana.AsanaObjectType.AV_COLLECT_TASKS;
import static org.apache.nifi.processors.asana.AsanaObjectType.AV_COLLECT_TASK_ATTACHMENTS;
import static org.apache.nifi.processors.asana.AsanaObjectType.AV_COLLECT_TEAM_MEMBERS;

@TriggerSerially
@PrimaryNodeOnly
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@WritesAttribute(attribute = GetAsanaObject.ASANA_GID, description = "Global ID of the object in Asana.")
@Tags({"asana", "source", "ingest"})
@CapabilityDescription("This processor collects data from Asana")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class GetAsanaObject extends AbstractProcessor {

    protected static final String ASANA_GID = "asana.gid";
    protected static final String REL_NAME_NEW = "new";
    protected static final String REL_NAME_UPDATED = "updated";
    protected static final String REL_NAME_REMOVED = "removed";

    protected static final PropertyDescriptor PROP_ASANA_CLIENT_SERVICE = new Builder()
            .name("Asana Client Service")
            .description("Specify which controller service to use for accessing Asana.")
            .required(true)
            .identifiesControllerService(AsanaClientProviderService.class)
            .build();

    protected static final PropertyDescriptor PROP_DISTRIBUTED_CACHE_SERVICE = new Builder()
            .name("Distributed Cache Service")
            .description("Cache service to store fetched item fingerprints. These, from the last successful query"
                    + " are stored, in order to enable incremental loading and change detection.")
            .required(true)
            .identifiesControllerService(DistributedMapCacheClient.class)
            .build();

    protected static final PropertyDescriptor PROP_ASANA_OBJECT_TYPE = new Builder()
            .name("Object Type")
            .description("Specify what kind of objects to be collected from Asana")
            .required(true)
            .allowableValues(AsanaObjectType.class)
            .defaultValue(AV_COLLECT_TASKS)
            .build();

    protected static final PropertyDescriptor PROP_ASANA_PROJECT = new Builder()
            .name("Project Name")
            .description("Fetch only objects in this project. Case sensitive.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(
                PROP_ASANA_OBJECT_TYPE,
                    AV_COLLECT_TASKS,
                    AV_COLLECT_TASK_ATTACHMENTS,
                    AV_COLLECT_PROJECT_MEMBERS,
                    AV_COLLECT_STORIES,
                    AV_COLLECT_PROJECT_STATUS_UPDATES,
                    AV_COLLECT_PROJECT_STATUS_ATTACHMENTS,
                    AV_COLLECT_PROJECT_EVENTS)
            .build();

    protected static final PropertyDescriptor PROP_ASANA_SECTION = new Builder()
            .name("Section Name")
            .description("Fetch only objects in this section. Case sensitive.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(PROP_ASANA_OBJECT_TYPE,
                    AV_COLLECT_TASKS,
                    AV_COLLECT_TASK_ATTACHMENTS,
                    AV_COLLECT_STORIES)
            .build();

    protected static final PropertyDescriptor PROP_ASANA_TAG = new Builder()
            .name("Tag")
            .description("Fetch only objects having this tag. Case sensitive.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(PROP_ASANA_OBJECT_TYPE,
                    AV_COLLECT_TASKS,
                    AV_COLLECT_TASK_ATTACHMENTS,
                    AV_COLLECT_STORIES)
            .build();

    protected static final PropertyDescriptor PROP_ASANA_TEAM_NAME = new Builder()
            .name("Team")
            .description("Team name. Case sensitive.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_TEAM_MEMBERS)
            .build();

    protected static final PropertyDescriptor PROP_ASANA_OUTPUT_BATCH_SIZE = new Builder()
            .name("Output Batch Size")
            .description("The number of items batched together in a single Flow File. If set to 1 (default), then each item is"
                    + " transferred in a separate Flow File and each will have an asana.gid attribute, to help identifying"
                    + " the fetched item on the server side, if needed. If the batch size is greater than 1, then the"
                    + " specified amount of items are batched together in a single Flow File as a Json array, and the"
                    + " Flow Files won't have the asana.gid attribute.")
            .defaultValue("1")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    protected static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            PROP_ASANA_CLIENT_SERVICE,
            PROP_DISTRIBUTED_CACHE_SERVICE,
            PROP_ASANA_OBJECT_TYPE,
            PROP_ASANA_PROJECT,
            PROP_ASANA_SECTION,
            PROP_ASANA_TEAM_NAME,
            PROP_ASANA_TAG,
            PROP_ASANA_OUTPUT_BATCH_SIZE
    );

    protected static final Relationship REL_NEW = new Relationship.Builder()
            .name(REL_NAME_NEW)
            .description("Newly collected objects are routed to this relationship.")
            .build();

    protected static final Relationship REL_UPDATED = new Relationship.Builder()
            .name(REL_NAME_UPDATED)
            .description("Objects that have already been collected earlier, but were updated since, are routed to this relationship.")
            .build();

    protected static final Relationship REL_REMOVED = new Relationship.Builder()
            .name(REL_NAME_REMOVED)
            .description("Notification about deleted objects are routed to this relationship. "
                    + "Flow files will not have any payload. IDs of the resources no longer exist "
                    + "are carried by the asana.gid attribute of the generated FlowFiles.")
            .build();

    protected static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_NEW,
            REL_UPDATED,
            REL_REMOVED
    );
    protected static final GenericObjectSerDe<String> STATE_MAP_KEY_SERIALIZER = new GenericObjectSerDe<>();
    protected static final GenericObjectSerDe<Map<String, String>> STATE_MAP_VALUE_SERIALIZER = new GenericObjectSerDe<>();

    private volatile AsanaObjectFetcher objectFetcher;
    private volatile Integer batchSize;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws InitializationException {
        AsanaClientProviderService controllerService = context.getProperty(PROP_ASANA_CLIENT_SERVICE).asControllerService(AsanaClientProviderService.class);
        AsanaClient client = controllerService.createClient();
        batchSize = context.getProperty(PROP_ASANA_OUTPUT_BATCH_SIZE).asInteger();

        try {
            getLogger().debug("Initializing object fetcher...");
            objectFetcher = createObjectFetcher(context, client);
        } catch (Exception e) {
            throw new InitializationException(e);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        Map<String, String> processorState = recoverState(context).orElse(Collections.emptyMap());
        try {
            getLogger().debug("Attempting to load state: {}", processorState);
            objectFetcher.loadState(processorState);
        } catch (Exception e) {
            getLogger().info("Failed to recover state. Falling back to clean start.");
            objectFetcher.clearState();
        }
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("Initial state: {}", objectFetcher.saveState());
        }

        int transferCount = 0;

        if (batchSize == 1) {
            AsanaObject asanaObject;
            while ((asanaObject = objectFetcher.fetchNext()) != null) {
                final Map<String, String> attributes = new HashMap<>(2);
                attributes.put(CoreAttributes.MIME_TYPE.key(), ContentType.APPLICATION_JSON.getMimeType());
                attributes.put(ASANA_GID, asanaObject.getGid());
                FlowFile flowFile = createFlowFileWithStringPayload(session, asanaObject.getContent());
                flowFile = session.putAllAttributes(flowFile, attributes);
                transferFlowFileByAsanaObjectState(session, asanaObject.getState(), flowFile);
                transferCount++;
            }
        } else {
            final Map<AsanaObjectState, Collection<String>> flowFileContents = new HashMap<>();
            flowFileContents.put(AsanaObjectState.NEW, new ArrayList<>());
            flowFileContents.put(AsanaObjectState.UPDATED, new ArrayList<>());
            flowFileContents.put(AsanaObjectState.REMOVED, new ArrayList<>());

            AsanaObject asanaObject;
            while ((asanaObject = objectFetcher.fetchNext()) != null) {
                AsanaObjectState state = asanaObject.getState();
                Collection<String> buffer = flowFileContents.get(state);
                buffer.add(asanaObject.getContent());
                if (buffer.size() == batchSize) {
                    transferBatchedItemsFromBuffer(session, state, buffer);
                    transferCount++;
                    buffer.clear();
                }
            }
            for (Entry<AsanaObjectState, Collection<String>> entry : flowFileContents.entrySet()) {
                if (!entry.getValue().isEmpty()) {
                    transferBatchedItemsFromBuffer(session, entry.getKey(), entry.getValue());
                    transferCount++;
                }
            }
        }

        if (transferCount == 0) {
            context.yield();
            getLogger().debug("Yielding, as there are no new FlowFiles.");
        }

        session.commitAsync();
        Map<String, String> state = objectFetcher.saveState();
        persistState(state, context);
        objectFetcher.clearState();

        getLogger().debug("New state after transferring {} FlowFiles: {}", transferCount, state);
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        config.renameProperty("asana-controller-service", PROP_ASANA_CLIENT_SERVICE.getName());
        config.renameProperty("distributed-cache-service", PROP_DISTRIBUTED_CACHE_SERVICE.getName());
        config.renameProperty("asana-object-type", PROP_ASANA_OBJECT_TYPE.getName());
        config.renameProperty("asana-project-name", PROP_ASANA_PROJECT.getName());
        config.renameProperty("asana-section-name", PROP_ASANA_SECTION.getName());
        config.renameProperty("asana-tag-name", PROP_ASANA_TAG.getName());
        config.renameProperty("asana-team-name", PROP_ASANA_TEAM_NAME.getName());
        config.renameProperty("asana-output-batch-size", PROP_ASANA_OUTPUT_BATCH_SIZE.getName());
    }

    private void transferBatchedItemsFromBuffer(ProcessSession session, AsanaObjectState state, Collection<String> buffer) {
        FlowFile flowFile = createFlowFileWithStringPayload(session, format("[%s]", join(",", buffer)));
        flowFile = session.putAllAttributes(flowFile,
                singletonMap(CoreAttributes.MIME_TYPE.key(),
                        ContentType.APPLICATION_JSON.getMimeType()));
        transferFlowFileByAsanaObjectState(session, state, flowFile);
    }

    private static void transferFlowFileByAsanaObjectState(ProcessSession session, AsanaObjectState state, FlowFile flowFile) {
        switch (state) {
            case NEW:
                session.transfer(flowFile, REL_NEW);
                break;
            case UPDATED:
                session.transfer(flowFile, REL_UPDATED);
                break;
            case REMOVED:
                session.transfer(flowFile, REL_REMOVED);
                break;
        }
    }

    protected AsanaObjectFetcher createObjectFetcher(final ProcessContext context, AsanaClient client) {
        final AsanaObjectType objectType = context.getProperty(PROP_ASANA_OBJECT_TYPE).asAllowableValue(AsanaObjectType.class);
        final String projectName = getProjectName(context, objectType);
        final String sectionName = getSectionName(context, objectType);
        final String tagName = getTagName(context, objectType);
        final String teamName = getTeamName(context, objectType);

        return switch (objectType) {
            case AV_COLLECT_TASKS -> new AsanaTaskFetcher(client, projectName, sectionName, tagName);
            case AV_COLLECT_PROJECTS -> new AsanaProjectFetcher(client);
            case AV_COLLECT_PROJECT_EVENTS -> new AsanaProjectEventFetcher(client, projectName);
            case AV_COLLECT_PROJECT_MEMBERS -> new AsanaProjectMembershipFetcher(client, projectName);
            case AV_COLLECT_PROJECT_STATUS_ATTACHMENTS -> new AsanaProjectStatusAttachmentFetcher(client, projectName);
            case AV_COLLECT_PROJECT_STATUS_UPDATES -> new AsanaProjectStatusFetcher(client, projectName);
            case AV_COLLECT_STORIES -> new AsanaStoryFetcher(client, projectName, sectionName, tagName);
            case AV_COLLECT_TAGS -> new AsanaTagFetcher(client);
            case AV_COLLECT_TASK_ATTACHMENTS -> new AsanaTaskAttachmentFetcher(client, projectName, sectionName, tagName);
            case AV_COLLECT_TEAMS -> new AsanaTeamFetcher(client);
            case AV_COLLECT_TEAM_MEMBERS -> new AsanaTeamMemberFetcher(client, teamName);
            case AV_COLLECT_USERS -> new AsanaUserFetcher(client);
        };
    }

    private Optional<Map<String, String>> recoverState(final ProcessContext context) {
        final DistributedMapCacheClient client = getDistributedMapCacheClient(context);
        try {
            final Map<String, String> result = client.get(getIdentifier(), STATE_MAP_KEY_SERIALIZER,
                    STATE_MAP_VALUE_SERIALIZER);
            return Optional.ofNullable(result);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private FlowFile createFlowFileWithStringPayload(ProcessSession session, String payload) {
        byte[] data = payload.getBytes(StandardCharsets.UTF_8);
        return session.importFrom(new ByteArrayInputStream(data), session.create());
    }

    private void persistState(final Map<String, String> state, final ProcessContext context) {
        final DistributedMapCacheClient client = getDistributedMapCacheClient(context);
        try {
            client.put(getIdentifier(), state, STATE_MAP_KEY_SERIALIZER, STATE_MAP_VALUE_SERIALIZER);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected String getProjectName(ProcessContext context, AsanaObjectType objectType) {
        return switch (objectType) {
            case AV_COLLECT_TASKS,
                 AV_COLLECT_TASK_ATTACHMENTS,
                 AV_COLLECT_PROJECT_MEMBERS,
                 AV_COLLECT_STORIES,
                 AV_COLLECT_PROJECT_STATUS_UPDATES,
                 AV_COLLECT_PROJECT_STATUS_ATTACHMENTS,
                 AV_COLLECT_PROJECT_EVENTS -> context.getProperty(PROP_ASANA_PROJECT).getValue();
            case AV_COLLECT_PROJECTS, AV_COLLECT_TEAM_MEMBERS, AV_COLLECT_TAGS, AV_COLLECT_USERS, AV_COLLECT_TEAMS -> null;
        };
    }

    protected String getSectionName(ProcessContext context, AsanaObjectType objectType) {
        return switch (objectType) {
            case AV_COLLECT_TASKS,
                 AV_COLLECT_TASK_ATTACHMENTS,
                 AV_COLLECT_STORIES -> context.getProperty(PROP_ASANA_SECTION).getValue();
            case AV_COLLECT_PROJECTS, AV_COLLECT_TAGS, AV_COLLECT_USERS, AV_COLLECT_PROJECT_MEMBERS, AV_COLLECT_TEAMS,
                 AV_COLLECT_TEAM_MEMBERS, AV_COLLECT_PROJECT_STATUS_UPDATES, AV_COLLECT_PROJECT_STATUS_ATTACHMENTS,
                 AV_COLLECT_PROJECT_EVENTS -> null;
        };
    }

    protected String getTagName(ProcessContext context, AsanaObjectType objectType) {
        return switch (objectType) {
            case AV_COLLECT_TASKS,
                 AV_COLLECT_TASK_ATTACHMENTS,
                 AV_COLLECT_STORIES -> context.getProperty(PROP_ASANA_TAG).getValue();
            case AV_COLLECT_PROJECTS, AV_COLLECT_TAGS, AV_COLLECT_USERS, AV_COLLECT_PROJECT_MEMBERS, AV_COLLECT_TEAMS,
                 AV_COLLECT_TEAM_MEMBERS, AV_COLLECT_PROJECT_STATUS_UPDATES, AV_COLLECT_PROJECT_STATUS_ATTACHMENTS,
                 AV_COLLECT_PROJECT_EVENTS -> null;
        };
    }

    protected String getTeamName(ProcessContext context, AsanaObjectType objectType) {
        return switch (objectType) {
            case AV_COLLECT_TEAM_MEMBERS -> context.getProperty(PROP_ASANA_TEAM_NAME).getValue();
            case AV_COLLECT_TASKS, AV_COLLECT_TASK_ATTACHMENTS, AV_COLLECT_PROJECTS, AV_COLLECT_TAGS, AV_COLLECT_USERS,
                 AV_COLLECT_PROJECT_MEMBERS, AV_COLLECT_TEAMS, AV_COLLECT_STORIES, AV_COLLECT_PROJECT_STATUS_UPDATES,
                 AV_COLLECT_PROJECT_STATUS_ATTACHMENTS, AV_COLLECT_PROJECT_EVENTS -> null;
        };
    }

    private static DistributedMapCacheClient getDistributedMapCacheClient(ProcessContext context) {
        return context.getProperty(PROP_DISTRIBUTED_CACHE_SERVICE).asControllerService(DistributedMapCacheClient.class);
    }
}
