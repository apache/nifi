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
package org.apache.nifi.processors.box;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxAPIException;
import com.box.sdk.BoxAPIResponseException;
import com.box.sdk.BoxCollaboration;
import com.box.sdk.BoxCollaborator;
import com.box.sdk.BoxFile;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.box.controllerservices.BoxClientService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_CODE;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_CODE_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.ID;
import static org.apache.nifi.processors.box.BoxFileAttributes.ID_DESC;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"box", "storage", "collaboration", "permissions", "sharing"})
@CapabilityDescription("Retrieves all collaborators on a Box file and adds the collaboration information to the FlowFile's attributes.")
@SeeAlso({FetchBoxFile.class, ListBoxFile.class})
@WritesAttributes({
        @WritesAttribute(attribute = ID, description = ID_DESC),
        @WritesAttribute(attribute = "box.collaborations.<status>.<role>.users.ids", description = "Comma-separated list of user collaborator IDs by status and role"),
        @WritesAttribute(attribute = "box.collaborations.<status>.<role>.users.logins", description = "Comma-separated list of user collaborator logins by status and role"),
        @WritesAttribute(attribute = "box.collaborations.<status>.<role>.groups.ids", description = "Comma-separated list of group collaborator IDs by status and role"),
        @WritesAttribute(attribute = "box.collaborations.<status>.<role>.groups.emails", description = "Comma-separated list of group collaborator emails by status and role"),
        @WritesAttribute(attribute = "box.collaborations.count", description = "Total number of collaborations on the file"),
        @WritesAttribute(attribute = ERROR_CODE, description = ERROR_CODE_DESC),
        @WritesAttribute(attribute = ERROR_MESSAGE, description = ERROR_MESSAGE_DESC)
})
public class GetBoxFileCollaborators extends AbstractProcessor {

    public static final PropertyDescriptor FILE_ID = new PropertyDescriptor.Builder()
            .name("File ID")
            .description("The ID of the Box file to retrieve collaborators for")
            .required(true)
            .defaultValue("${box.id}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ROLES = new PropertyDescriptor.Builder()
            .name("Roles")
            .description("A comma-separated list of collaboration roles to retrieve. Available roles: editor, viewer, previewer, " +
                    "uploader, previewer uploader, viewer uploader, co-owner, owner")
            .required(true)
            .defaultValue("editor")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor STATUSES = new PropertyDescriptor.Builder()
            .name("Statuses")
            .description("A comma-separated list of collaboration statuses to retrieve. Available statuses: accepted, pending, rejected")
            .required(true)
            .defaultValue("accepted")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that have been successfully processed will be routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that encounter errors during processing will be routed to this relationship")
            .build();

    public static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not.found")
            .description("FlowFiles for which the specified Box file was not found")
            .build();

    public static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_NOT_FOUND
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BoxClientService.BOX_CLIENT_SERVICE,
            FILE_ID,
            ROLES,
            STATUSES
    );

    private volatile BoxAPIConnection boxAPIConnection;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        BoxClientService boxClientService = context.getProperty(BoxClientService.BOX_CLIENT_SERVICE).asControllerService(BoxClientService.class);
        boxAPIConnection = boxClientService.getBoxApiConnection();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String fileId = context.getProperty(FILE_ID).evaluateAttributeExpressions(flowFile).getValue();
        final String roles = context.getProperty(ROLES).evaluateAttributeExpressions(flowFile).getValue();
        final String statuses = context.getProperty(STATUSES).evaluateAttributeExpressions(flowFile).getValue();

        try {
            flowFile = fetchCollaborations(fileId, roles, statuses, session, flowFile);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final BoxAPIResponseException e) {
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            flowFile = session.putAttribute(flowFile, ERROR_CODE, String.valueOf(e.getResponseCode()));

            if (e.getResponseCode() == 404) {
                getLogger().warn("Box file with ID {} was not found.", fileId);
                session.transfer(flowFile, REL_NOT_FOUND);
            } else {
                getLogger().error("Failed to retrieve Box file collaborations for file [{}]", fileId, e);
                session.transfer(flowFile, REL_FAILURE);
            }
        } catch (final BoxAPIException e) {
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            flowFile = session.putAttribute(flowFile, ERROR_CODE, String.valueOf(e.getResponseCode()));
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    /**
     * Creates a BoxFile instance for a given file ID.
     *
     * @param fileId the ID of the file
     * @return BoxFile instance
     */
    protected BoxFile getBoxFile(final String fileId) {
        return new BoxFile(boxAPIConnection, fileId);
    }

    private FlowFile fetchCollaborations(final String fileId,
                                         final String roleFilter,
                                         final String statusFilter,
                                         final ProcessSession session,
                                         final FlowFile flowFile) {
        final BoxFile boxFile = getBoxFile(fileId);
        final Iterable<BoxCollaboration.Info> collaborations = boxFile.getAllFileCollaborations();

        // Parse role and status filters
        final Set<String> allowedRoles = Arrays.stream(roleFilter.toLowerCase().split(","))
                .map(String::trim)
                .collect(Collectors.toSet());

        final Set<String> allowedStatuses = Arrays.stream(statusFilter.toLowerCase().split(","))
                .map(String::trim)
                .collect(Collectors.toSet());

        // Map to store values for each combination of status, role, entity type and attribute type
        final Map<String, List<String>> collabAttributeMap = new HashMap<>();

        int count = 0;
        for (final BoxCollaboration.Info collabInfo : collaborations) {
            count++;

            final String status = collabInfo.getStatus().toString().toLowerCase();
            final String role = collabInfo.getRole().toString().toLowerCase();

            // Skip if not in the allowed roles or statuses
            if (!allowedRoles.contains(role) || !allowedStatuses.contains(status)) {
                continue;
            }

            final boolean isUser = collabInfo.getAccessibleBy().getType().equals(BoxCollaborator.CollaboratorType.USER);
            final String entityType = isUser ? "users" : "groups";
            final String collabId = collabInfo.getAccessibleBy().getID();
            final String login = collabInfo.getAccessibleBy().getLogin();

            // Build keys for the various attributes
            final String idKey = String.format("%s.%s.%s.ids", status, role, entityType);
            final String loginKey = isUser
                    ? String.format("%s.%s.users.logins", status, role)
                    : String.format("%s.%s.groups.emails", status, role);

            // Initialize lists if they don't exist
            collabAttributeMap.computeIfAbsent(idKey, k -> new ArrayList<>()).add(collabId);

            if (login != null) {
                collabAttributeMap.computeIfAbsent(loginKey, k -> new ArrayList<>()).add(login);
            }
        }

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ID, fileId);
        attributes.put("box.collaborations.count", String.valueOf(count));

        // Add all collected attributes to the flowfile
        collabAttributeMap.forEach((key, values) ->
                addAttributeIfNotEmpty(attributes, "box.collaborations." + key, values));

        return session.putAllAttributes(flowFile, attributes);
    }

    private void addAttributeIfNotEmpty(final Map<String, String> attributes,
                                        final String key,
                                        final List<String> values) {
        if (values != null && !values.isEmpty()) {
            attributes.put(key, String.join(",", values));
        }
    }
}