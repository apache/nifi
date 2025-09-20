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
import com.box.sdk.BoxGroup;
import com.box.sdk.BoxGroupMembership;
import com.box.sdk.BoxUser;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
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

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static java.util.stream.Collectors.joining;
import static org.apache.nifi.annotation.behavior.InputRequirement.Requirement.INPUT_REQUIRED;
import static org.apache.nifi.processors.box.BoxGroupAttributes.ERROR_CODE;
import static org.apache.nifi.processors.box.BoxGroupAttributes.ERROR_MESSAGE;
import static org.apache.nifi.processors.box.BoxGroupAttributes.GROUP_USER_IDS;
import static org.apache.nifi.processors.box.BoxGroupAttributes.GROUP_USER_LOGINS;

@SideEffectFree
@InputRequirement(INPUT_REQUIRED)
@Tags({"box", "storage", "metadata"})
@CapabilityDescription("Retrieves members for a Box Group and writes their details in FlowFile attributes.")
@ReadsAttributes({
        @ReadsAttribute(attribute = BoxGroupAttributes.GROUP_ID, description = "The ID of the Group to retrieve members for."),
})
@WritesAttributes({
        @WritesAttribute(attribute = GROUP_USER_IDS, description = "A comma-separated list of user IDs in the group."),
        @WritesAttribute(attribute = GROUP_USER_LOGINS, description = "A comma-separated list of user Logins (emails) in the group."),
        @WritesAttribute(attribute = ERROR_CODE, description = "An http error code returned by Box."),
        @WritesAttribute(attribute = ERROR_MESSAGE, description = "An error message returned by Box."),
})
public class GetBoxGroupMembers extends AbstractBoxProcessor {

    static final PropertyDescriptor GROUP_ID = new PropertyDescriptor.Builder()
            .name("Group ID")
            .description("The ID of the Group to retrieve members for")
            .required(true)
            .defaultValue("${%s}".formatted(BoxGroupAttributes.GROUP_ID))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BOX_CLIENT_SERVICE,
            GROUP_ID
    );

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The FlowFile will be routed here after successfully retrieving Group members.")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("The FlowFile will be routed here when Group memberships retrieval was attempted but failed.")
            .build();

    static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not.found")
            .description("The FlowFile will be routed here when the Group was not found.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_NOT_FOUND
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    private volatile BoxAPIConnection boxAPIConnection;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final BoxClientService boxClientService = context.getProperty(BOX_CLIENT_SERVICE).asControllerService(BoxClientService.class);
        boxAPIConnection = boxClientService.getBoxApiConnection();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String groupId = context.getProperty(GROUP_ID).evaluateAttributeExpressions(flowFile).getValue();
        try {
            final Collection<BoxGroupMembership.Info> memberships = retrieveGroupMemberships(groupId);

            final String userIDs = extractUserProperty(memberships, BoxUser.Info::getID);
            final String userLogins = extractUserProperty(memberships, BoxUser.Info::getLogin);

            session.putAttribute(flowFile, GROUP_USER_IDS, userIDs);
            session.putAttribute(flowFile, GROUP_USER_LOGINS, userLogins);
            session.transfer(flowFile, REL_SUCCESS);

        } catch (final BoxAPIException e) {
            session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            session.putAttribute(flowFile, ERROR_CODE, String.valueOf(e.getResponseCode()));

            if (e.getResponseCode() == 404) {
                getLogger().warn("Box Group with ID {} was not found.", groupId);
                session.transfer(flowFile, REL_NOT_FOUND);
            } else {
                getLogger().error("Failed to retrieve Box Group for ID: {}, file [{}]", groupId, flowFile, e);
                session.transfer(flowFile, REL_FAILURE);
            }
        } catch (final ProcessException e) {
            throw e;
        } catch (final RuntimeException e) {
            getLogger().error("Failed to retrieve Box Group for ID: {}, file [{}]", groupId, flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private String extractUserProperty(final Collection<BoxGroupMembership.Info> memberships, final Function<BoxUser.Info, String> userPropertyExtractor) {
        return memberships.stream()
                .map(BoxGroupMembership.Info::getUser)
                .map(userPropertyExtractor)
                .collect(joining(","));
    }

    // Package-private for testing.
    Collection<BoxGroupMembership.Info> retrieveGroupMemberships(final String groupId) {
        return new BoxGroup(boxAPIConnection, groupId).getMemberships();
    }
}
