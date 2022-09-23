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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.http.entity.ContentType;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.controller.asana.AsanaClient;
import org.apache.nifi.controller.asana.AsanaClientServiceApi;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.asana.utils.AsanaObject;
import org.apache.nifi.processors.asana.utils.AsanaObjectFetcher;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@TriggerSerially
@Stateful(scopes = {Scope.LOCAL}, description = "Fingerprints of items in the last successful query are stored in order to enable incremental loading and change detection.")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@WritesAttribute(attribute = GetAsanaObject.ASANA_GID, description = "Global ID of the object in Asana.")
@Tags({"asana", "source", "connector", "ingest"})
@CapabilityDescription("This processor collects data from Asana")
public class GetAsanaObject extends AbstractProcessor {

    protected static final String ASANA_GID = "asana.gid";
    protected static final String AV_NAME_COLLECT_TASKS = "asana-collect-tasks";
    protected static final String AV_NAME_COLLECT_TASK_ATTACHMENTS = "asana-collect-task-attachments";
    protected static final String AV_NAME_COLLECT_PROJECTS = "asana-collect-projects";
    protected static final String AV_NAME_COLLECT_TAGS = "asana-collect-tags";
    protected static final String AV_NAME_COLLECT_USERS = "asana-collect-users";
    protected static final String AV_NAME_COLLECT_PROJECT_MEMBERS = "asana-collect-project-members";
    protected static final String AV_NAME_COLLECT_TEAMS = "asana-collect-teams";
    protected static final String AV_NAME_COLLECT_TEAM_MEMBERS = "asana-collect-team-members";
    protected static final String AV_NAME_COLLECT_STORIES = "asana-collect-stories";
    protected static final String AV_NAME_COLLECT_PROJECT_STATUS_UPDATES = "asana-collect-project-status-updates";
    protected static final String AV_NAME_COLLECT_PROJECT_STATUS_ATTACHMENTS = "asana-collect-project-status-attachments";
    protected static final String AV_NAME_COLLECT_PROJECT_EVENTS = "asana-collect-project-events";
    protected static final String ASANA_CONTROLLER_SERVICE = "asana-controller-service";
    protected static final String ASANA_OBJECT_TYPE = "asana-object-type";
    protected static final String ASANA_PROJECT_NAME = "asana-project-name";
    protected static final String ASANA_SECTION_NAME = "asana-section-name";
    protected static final String ASANA_TAG_NAME = "asana-tag-name";
    protected static final String ASANA_TEAM_NAME = "asana-team-name";
    protected static final String ASANA_OUTPUT_BATCH_SIZE = "asana-output-batch-size";
    protected static final String REL_NAME_NEW = "new";
    protected static final String REL_NAME_UPDATED = "updated";
    protected static final String REL_NAME_REMOVED = "removed";

    protected static final AllowableValue AV_COLLECT_TASKS = new AllowableValue(
            AV_NAME_COLLECT_TASKS,
            "Tasks",
            "Collect tasks matching to the specified conditions.");

    protected static final AllowableValue AV_COLLECT_TASK_ATTACHMENTS = new AllowableValue(
            AV_NAME_COLLECT_TASK_ATTACHMENTS,
            "Task attachments",
            "Collect attached files of tasks matching to the specified conditions."
    );

    protected static final AllowableValue AV_COLLECT_PROJECTS = new AllowableValue(
            AV_NAME_COLLECT_PROJECTS,
            "Projects",
            "Collect projects of the workspace."
    );

    protected static final AllowableValue AV_COLLECT_TAGS = new AllowableValue(
            AV_NAME_COLLECT_TAGS,
            "Tags",
            "Collect tags of the workspace."
    );

    protected static final AllowableValue AV_COLLECT_USERS = new AllowableValue(
            AV_NAME_COLLECT_USERS,
            "Users",
            "Collect users assigned to the workspace."
    );

    protected static final AllowableValue AV_COLLECT_PROJECT_MEMBERS = new AllowableValue(
            AV_NAME_COLLECT_PROJECT_MEMBERS,
            "Members of a project",
            "Collect users assigned to the specified project."
    );

    protected static final AllowableValue AV_COLLECT_TEAMS = new AllowableValue(
            AV_NAME_COLLECT_TEAMS,
            "Teams",
            "Collect teams of the workspace."
    );

    protected static final AllowableValue AV_COLLECT_TEAM_MEMBERS = new AllowableValue(
            AV_NAME_COLLECT_TEAM_MEMBERS,
            "Team members",
            "Collect users assigned to the specified team."
    );

    protected static final AllowableValue AV_COLLECT_STORIES = new AllowableValue(
            AV_NAME_COLLECT_STORIES,
            "Stories of tasks",
            "Collect stories (comments) of of tasks matching to the specified conditions."
    );

    protected static final AllowableValue AV_COLLECT_PROJECT_STATUS_UPDATES = new AllowableValue(
            AV_NAME_COLLECT_PROJECT_STATUS_UPDATES,
            "Status updates of a project",
            "Collect status updates of the specified project."
    );

    protected static final AllowableValue AV_COLLECT_PROJECT_STATUS_ATTACHMENTS = new AllowableValue(
            AV_NAME_COLLECT_PROJECT_STATUS_ATTACHMENTS,
            "Attachments of status updates",
            "Collect attached files of project status updates."
    );

    protected static final AllowableValue AV_COLLECT_PROJECT_EVENTS = new AllowableValue(
            AV_NAME_COLLECT_PROJECT_EVENTS,
            "Events of a project",
            "Collect various events happening on the specified project and on its' tasks."
    );

    protected static final PropertyDescriptor PROP_ASANA_CONTROLLER_SERVICE = new PropertyDescriptor.Builder()
            .name(ASANA_CONTROLLER_SERVICE)
            .displayName("Controller service")
            .description("Specify which controller service to use for accessing Asana.")
            .required(true)
            .identifiesControllerService(AsanaClientServiceApi.class)
            .build();

    protected static final PropertyDescriptor PROP_ASANA_OBJECT_TYPE = new PropertyDescriptor.Builder()
            .name(ASANA_OBJECT_TYPE)
            .displayName("Object type to be collected")
            .description("Specify what kind of objects to be collected from Asana")
            .required(true)
            .allowableValues(
                    AV_COLLECT_TASKS,
                    AV_COLLECT_TASK_ATTACHMENTS,
                    AV_COLLECT_PROJECTS,
                    AV_COLLECT_TAGS,
                    AV_COLLECT_USERS,
                    AV_COLLECT_PROJECT_MEMBERS,
                    AV_COLLECT_TEAMS,
                    AV_COLLECT_TEAM_MEMBERS,
                    AV_COLLECT_STORIES,
                    AV_COLLECT_PROJECT_STATUS_UPDATES,
                    AV_COLLECT_PROJECT_STATUS_ATTACHMENTS,
                    AV_COLLECT_PROJECT_EVENTS)
            .defaultValue(AV_COLLECT_TASKS.getValue())
            .build();

    protected static final PropertyDescriptor PROP_ASANA_PROJECT = new PropertyDescriptor.Builder()
            .name(ASANA_PROJECT_NAME)
            .displayName("Project name")
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

    protected static final PropertyDescriptor PROP_ASANA_SECTION = new PropertyDescriptor.Builder()
            .name(ASANA_SECTION_NAME)
            .displayName("Section name")
            .description("Fetch only objects in this section. Case sensitive.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_TASKS, AV_COLLECT_TASK_ATTACHMENTS, AV_COLLECT_STORIES)
            .build();

    protected static final PropertyDescriptor PROP_ASANA_TAG = new PropertyDescriptor.Builder()
            .name(ASANA_TAG_NAME)
            .displayName("Tag")
            .description("Fetch only objects having this tag. Case sensitive.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_TASKS, AV_COLLECT_TASK_ATTACHMENTS, AV_COLLECT_STORIES)
            .build();

    protected static final PropertyDescriptor PROP_ASANA_TEAM_NAME = new PropertyDescriptor.Builder()
            .name(ASANA_TEAM_NAME)
            .displayName("Team")
            .description("Team name. Case sensitive.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_TEAM_MEMBERS)
            .build();

    protected static final PropertyDescriptor PROP_ASANA_OUTPUT_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name(ASANA_OUTPUT_BATCH_SIZE)
            .displayName("Output Batch Size")
            .description("The number of output FlowFiles to queue before committing the process session. When set to zero, the session will be committed when all result set rows "
                    + "have been processed and the output FlowFiles are ready for transfer to the downstream relationship. For large result sets, this can cause a large burst of FlowFiles "
                    + "to be transferred at the end of processor execution. If this property is set, then when the specified number of FlowFiles are ready for transfer, then the session will "
                    + "be committed, thus releasing the FlowFiles to the downstream relationship.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    protected static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Lists.newArrayList(
            PROP_ASANA_CONTROLLER_SERVICE,
            PROP_ASANA_OBJECT_TYPE,
            PROP_ASANA_PROJECT,
            PROP_ASANA_SECTION,
            PROP_ASANA_TEAM_NAME,
            PROP_ASANA_TAG,
            PROP_ASANA_OUTPUT_BATCH_SIZE
    ));

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

    protected static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(Sets.newHashSet(
            REL_NEW,
            REL_UPDATED,
            REL_REMOVED
    ));

    final Scope STATE_STORAGE_SCOPE = Scope.LOCAL;

    AsanaClientServiceApi controllerService;
    AsanaObjectFetcher objectFetcher;
    private Integer batchSize;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @OnScheduled
    public synchronized void onScheduled(final ProcessContext context) throws InitializationException {
        controllerService = context.getProperty(PROP_ASANA_CONTROLLER_SERVICE).asControllerService(AsanaClientServiceApi.class);
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
    public synchronized void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        try {
            Map<String, String> state = recoverState(context).orElse(Collections.emptyMap());
            getLogger().debug("Attempting to load state: {}", state);
            objectFetcher.loadState(state);
        } catch (Exception e) {
            getLogger().info("Failed to recover state. Falling back to clean start.");
            objectFetcher.clearState();
        }
        getLogger().debug("Initial state: {}", objectFetcher.saveState());

        Collection<FlowFile> newItems = new ArrayList<>();
        Collection<FlowFile> updatedItems = new ArrayList<>();
        Collection<FlowFile> removedItems = new ArrayList<>();

        AsanaObject nextObject;
        while ((batchSize == 0 || (batchSize > (newItems.size() + updatedItems.size() + removedItems.size()))) && (nextObject = objectFetcher.fetchNext()) != null) {
            final Map<String, String> attributes = new HashMap<>(2);
            attributes.put(CoreAttributes.MIME_TYPE.key(), ContentType.APPLICATION_JSON.getMimeType());
            attributes.put(ASANA_GID, nextObject.getGid());
            FlowFile flowFile = createFlowFileWithStringPayload(session, nextObject.getContent());
            flowFile = session.putAllAttributes(flowFile, attributes);

            switch (nextObject.getState()) {
                case NEW:
                    newItems.add(flowFile);
                    break;
                case REMOVED:
                    removedItems.add(flowFile);
                    break;
                default:
                    updatedItems.add(flowFile);
            }
        }

        if (newItems.isEmpty() && updatedItems.isEmpty() && removedItems.isEmpty()) {
            context.yield();
            getLogger().debug("Yielding, as there are no new FlowFiles.");
            return;
        }

        session.transfer(newItems, REL_NEW);
        session.transfer(updatedItems, REL_UPDATED);
        session.transfer(removedItems, REL_REMOVED);

        session.commitAsync();
        Map<String, String> state = objectFetcher.saveState();
        try {
            persistState(state, context);
        } catch (IOException e) {
            throw new ProcessException(e);
        }
        getLogger().debug(
            "New state after transferring {} new, {} updated, and {} removed items: {}",
            newItems.size(), updatedItems.size(), removedItems.size(), state);
    }

    protected AsanaObjectFetcher createObjectFetcher(final ProcessContext context, AsanaClient client) {
        final String objectType = context.getProperty(PROP_ASANA_OBJECT_TYPE).getValue();
        final String projectName = context.getProperty(PROP_ASANA_PROJECT).getValue();
        final String sectionName = context.getProperty(PROP_ASANA_SECTION).getValue();
        final String teamName = context.getProperty(PROP_ASANA_TEAM_NAME).getValue();
        final String tagName = context.getProperty(PROP_ASANA_TAG).getValue();

        switch (objectType) {
            case AV_NAME_COLLECT_TASKS:
                return new AsanaTaskFetcher(client, projectName, sectionName, tagName);
            case AV_NAME_COLLECT_PROJECTS:
                return new AsanaProjectFetcher(client);
            case AV_NAME_COLLECT_PROJECT_EVENTS:
                return new AsanaProjectEventFetcher(client, projectName);
            case AV_NAME_COLLECT_PROJECT_MEMBERS:
                return new AsanaProjectMembershipFetcher(client, projectName);
            case AV_NAME_COLLECT_PROJECT_STATUS_ATTACHMENTS:
                return new AsanaProjectStatusAttachmentFetcher(client, projectName);
            case AV_NAME_COLLECT_PROJECT_STATUS_UPDATES:
                return new AsanaProjectStatusFetcher(client, projectName);
            case AV_NAME_COLLECT_STORIES:
                return new AsanaStoryFetcher(client, projectName, sectionName, tagName);
            case AV_NAME_COLLECT_TAGS:
                return new AsanaTagFetcher(client);
            case AV_NAME_COLLECT_TASK_ATTACHMENTS:
                return new AsanaTaskAttachmentFetcher(client, projectName, sectionName, tagName);
            case AV_NAME_COLLECT_TEAMS:
                return new AsanaTeamFetcher(client);
            case AV_NAME_COLLECT_TEAM_MEMBERS:
                return new AsanaTeamMemberFetcher(client, teamName);
            case AV_NAME_COLLECT_USERS:
                return new AsanaUserFetcher(client);
        }

        throw new RuntimeException("Cannot fetch objects of type: " + objectType);
    }

    private Optional<Map<String, String>> recoverState(final ProcessContext context) throws IOException {
        final StateMap stateMap = context.getStateManager().getState(STATE_STORAGE_SCOPE);
        if (stateMap.getVersion() == -1L || stateMap.toMap().isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(stateMap.toMap());
    }

    private FlowFile createFlowFileWithStringPayload(ProcessSession session, String payload) {
        byte[] data = payload.getBytes(StandardCharsets.UTF_8);
        return session.importFrom(new ByteArrayInputStream(data), session.create());
    }

    private void persistState(final Map<String, String> state, final ProcessContext context) throws IOException {
        context.getStateManager().setState(state, STATE_STORAGE_SCOPE);
    }
}
