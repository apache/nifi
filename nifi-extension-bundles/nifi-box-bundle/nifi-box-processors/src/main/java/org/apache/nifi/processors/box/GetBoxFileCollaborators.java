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

import com.box.sdkgen.box.errors.BoxAPIError;
import com.box.sdkgen.client.BoxClient;
import com.box.sdkgen.schemas.collaboration.Collaboration;
import com.box.sdkgen.schemas.collaborationaccessgrantee.CollaborationAccessGrantee;
import com.box.sdkgen.schemas.collaborations.Collaborations;
import com.box.sdkgen.schemas.groupmini.GroupMini;
import com.box.sdkgen.schemas.usercollaborations.UserCollaborations;
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
        // Always present (backward compatibility attributes)
        @WritesAttribute(attribute = "box.collaborations.<status>.users.ids", description = "Comma-separated list of user collaborator IDs by status"),
        @WritesAttribute(attribute = "box.collaborations.<status>.groups.ids", description = "Comma-separated list of group collaborator IDs by status"),
        @WritesAttribute(attribute = "box.collaborations.<status>.users.emails", description = "Comma-separated list of user collaborator emails by status"),
        @WritesAttribute(attribute = "box.collaborations.<status>.groups.emails", description = "Comma-separated list of group collaborator emails by status"),
        // New attributes (only present when both Roles and Statuses properties are set)
        @WritesAttribute(attribute = "box.collaborations.<status>.<role>.users.ids", description = "Comma-separated list of user collaborator IDs by status and role. " +
                "Only present when both Roles and Statuses properties are set."),
        @WritesAttribute(attribute = "box.collaborations.<status>.<role>.users.logins", description = "Comma-separated list of user collaborator logins by status and role. " +
                "Only present when both Roles and Statuses properties are set."),
        @WritesAttribute(attribute = "box.collaborations.<status>.<role>.groups.ids", description = "Comma-separated list of group collaborator IDs by status and role. " +
                "Only present when both Roles and Statuses properties are set."),
        @WritesAttribute(attribute = "box.collaborations.<status>.<role>.groups.emails", description = "Comma-separated list of group collaborator emails by status and role. " +
                "Only present when both Roles and Statuses properties are set."),
        @WritesAttribute(attribute = "box.collaborations.count", description = "Total number of collaborations on the file"),
        @WritesAttribute(attribute = ERROR_CODE, description = ERROR_CODE_DESC),
        @WritesAttribute(attribute = ERROR_MESSAGE, description = ERROR_MESSAGE_DESC)
})
public class GetBoxFileCollaborators extends AbstractBoxProcessor {

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
                    "uploader, previewer uploader, viewer uploader, co-owner, owner. If not specified, no filtering by role will be applied.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor STATUSES = new PropertyDescriptor.Builder()
            .name("Statuses")
            .description("A comma-separated list of collaboration statuses to retrieve. Available statuses: accepted, pending, rejected. " +
                    "If not specified, no filtering by status will be applied.")
            .required(false)
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
            BOX_CLIENT_SERVICE,
            FILE_ID,
            ROLES,
            STATUSES
    );

    private volatile BoxClient boxClient;

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
        BoxClientService boxClientService = context.getProperty(BOX_CLIENT_SERVICE).asControllerService(BoxClientService.class);
        boxClient = boxClientService.getBoxClient();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String fileId = context.getProperty(FILE_ID).evaluateAttributeExpressions(flowFile).getValue();

        // Get optional properties
        String roles = null;
        if (context.getProperty(ROLES).isSet()) {
            roles = context.getProperty(ROLES).evaluateAttributeExpressions(flowFile).getValue();
        }

        String statuses = null;
        if (context.getProperty(STATUSES).isSet()) {
            statuses = context.getProperty(STATUSES).evaluateAttributeExpressions(flowFile).getValue();
        }

        try {
            flowFile = fetchCollaborations(fileId, roles, statuses, session, flowFile);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final BoxAPIError e) {
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            if (e.getResponseInfo() != null) {
                flowFile = session.putAttribute(flowFile, ERROR_CODE, String.valueOf(e.getResponseInfo().getStatusCode()));
                if (e.getResponseInfo().getStatusCode() == 404) {
                    getLogger().warn("Box file with ID {} was not found.", fileId);
                    session.transfer(flowFile, REL_NOT_FOUND);
                    return;
                }
            }
            getLogger().error("Failed to retrieve Box file collaborations for file [{}]", fileId, e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private FlowFile fetchCollaborations(final String fileId,
                                         final String roleFilter,
                                         final String statusFilter,
                                         final ProcessSession session,
                                         final FlowFile flowFile) {

        final Collaborations collaborations = boxClient.getListCollaborations().getFileCollaborations(fileId);

        final Set<String> allowedRoles = parseFilter(roleFilter);
        final Set<String> allowedStatuses = parseFilter(statusFilter);

        final Map<String, List<String>> attributeValues = new HashMap<>();
        int count = processCollaborations(collaborations, allowedRoles, allowedStatuses, attributeValues);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ID, fileId);
        attributes.put("box.collaborations.count", String.valueOf(count));

        // Add all collected attributes with prefix
        attributeValues.forEach((key, values) ->
                addAttributeIfNotEmpty(attributes, "box.collaborations." + key, values));

        return session.putAllAttributes(flowFile, attributes);
    }

    /**
     * Parses a comma-separated string filter into a set of trimmed, lowercase values.
     *
     * @param filter the comma-separated filter string
     * @return a Set of filter values, or null if the input was null
     */
    private Set<String> parseFilter(final String filter) {
        if (filter == null) {
            return null;
        }
        return Arrays.stream(filter.toLowerCase().split(","))
                .map(String::trim)
                .collect(Collectors.toSet());
    }

    /**
     * Processes a list of collaborations, applying role and status filters,
     * and populates the attributeValues map with collaboration attributes.
     *
     * @param collaborations  the collaborations to process
     * @param allowedRoles    the set of allowed roles, or null for no filtering
     * @param allowedStatuses the set of allowed statuses, or null for no filtering
     * @param attributeValues the map to populate with collaboration attributes
     * @return the total count of collaborations processed (before filtering)
     */
    private int processCollaborations(final Collaborations collaborations,
                                      final Set<String> allowedRoles,
                                      final Set<String> allowedStatuses,
                                      final Map<String, List<String>> attributeValues) {
        int count = 0;

        if (collaborations.getEntries() == null) {
            return count;
        }

        for (final Collaboration collab : collaborations.getEntries()) {
            count++;

            final String status = collab.getStatus() != null ? collab.getStatus().getValue().toLowerCase() : "unknown";
            final String role = collab.getRole() != null ? collab.getRole().getValue().toLowerCase() : "unknown";

            // Skip if not in allowed roles or statuses
            if ((allowedRoles != null && !allowedRoles.contains(role))
                    || (allowedStatuses != null && !allowedStatuses.contains(status))) {
                continue;
            }

            // Process this collaboration's attributes
            processCollaboration(collab, status, role, allowedRoles != null && allowedStatuses != null, attributeValues);
        }

        return count;
    }

    /**
     * Processes a single collaboration and adds its attributes to the attributeValues map.
     *
     * @param collab          the collaboration to process
     * @param status          the lowercase status of the collaboration
     * @param role            the lowercase role of the collaboration
     * @param useNewFormat    whether to include new format attributes (with role)
     * @param attributeValues the map to populate with collaboration attributes
     */
    private void processCollaboration(final Collaboration collab,
                                      final String status,
                                      final String role,
                                      final boolean useNewFormat,
                                      final Map<String, List<String>> attributeValues) {
        final CollaborationAccessGrantee accessibleBy = collab.getAccessibleBy();
        if (accessibleBy == null) {
            return;
        }

        final boolean isUser = accessibleBy.isUserCollaborations();
        final String entityType = isUser ? "users" : "groups";
        String collabId = null;
        String login = null;

        if (isUser) {
            UserCollaborations user = accessibleBy.getUserCollaborations();
            collabId = user.getId();
            login = user.getLogin();
        } else if (accessibleBy.isGroupMini()) {
            GroupMini group = accessibleBy.getGroupMini();
            collabId = group.getId();
            login = group.getName();
        }

        if (collabId != null) {
            // Add backward compatibility attributes
            addToMap(attributeValues, status + "." + entityType + ".ids", collabId);

            if (login != null) {
                final String loginKey = isUser ? status + ".users.emails" : status + ".groups.emails";
                addToMap(attributeValues, loginKey, login);
            }

            // Add new format attributes if filters were provided
            if (useNewFormat) {
                addToMap(attributeValues, status + "." + role + "." + entityType + ".ids", collabId);

                if (login != null) {
                    final String loginKey = isUser
                            ? status + "." + role + ".users.logins" :
                            status + "." + role + ".groups.emails";
                    addToMap(attributeValues, loginKey, login);
                }
            }
        }
    }

    private void addToMap(final Map<String, List<String>> map, final String key, final String value) {
        map.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }

    private void addAttributeIfNotEmpty(final Map<String, String> attributes,
                                        final String key,
                                        final List<String> values) {
        if (values != null && !values.isEmpty()) {
            attributes.put(key, String.join(",", values));
        }
    }
}
