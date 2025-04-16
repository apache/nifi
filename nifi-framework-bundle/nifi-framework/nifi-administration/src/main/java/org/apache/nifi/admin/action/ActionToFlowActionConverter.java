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
package org.apache.nifi.admin.action;

import org.apache.nifi.action.Action;
import org.apache.nifi.action.FlowAction;
import org.apache.nifi.action.FlowActionAttribute;
import org.apache.nifi.action.RequestAction;
import org.apache.nifi.action.RequestDetails;
import org.apache.nifi.action.component.details.ComponentDetails;
import org.apache.nifi.action.component.details.ExtensionDetails;
import org.apache.nifi.action.component.details.RemoteProcessGroupDetails;
import org.apache.nifi.action.details.ActionDetails;
import org.apache.nifi.action.details.ConfigureDetails;
import org.apache.nifi.action.details.ConnectDetails;
import org.apache.nifi.action.details.MoveDetails;
import org.apache.nifi.action.details.PurgeDetails;

import java.util.HashMap;
import java.util.Map;

/**
 * Converts an {@link Action} to a {@link FlowAction} using {@link FlowActionAttribute} attributes.
 */
public class ActionToFlowActionConverter implements ActionConverter {

    @Override
    public FlowAction convert(final Action action) {
        final Map<String, String> attributes = new HashMap<>();
        populateActionAttributes(action, attributes);
        populateActionDetailsAttributes(action.getActionDetails(), attributes);
        populateComponentDetailsProperties(action.getComponentDetails(), attributes);
        return new StandardFlowAction(attributes);
    }

    private void populateActionAttributes(final Action action, final Map<String, String> attributes) {
        attributes.put(FlowActionAttribute.ACTION_TIMESTAMP.key(), action.getTimestamp().toInstant().toString());
        attributes.put(FlowActionAttribute.ACTION_USER_IDENTITY.key(), action.getUserIdentity());
        attributes.put(FlowActionAttribute.ACTION_SOURCE_ID.key(), action.getSourceId());
        attributes.put(FlowActionAttribute.ACTION_SOURCE_TYPE.key(), action.getSourceType().name());
        attributes.put(FlowActionAttribute.ACTION_OPERATION.key(), action.getOperation().name());

        if (action instanceof RequestAction requestAction) {
            populateRequestDetails(requestAction.getRequestDetails(), attributes);
        }
    }

    private void populateActionDetailsAttributes(final ActionDetails actionDetails, final Map<String, String> attributes) {
        switch (actionDetails) {
            case ConfigureDetails configureDetails -> attributes.put(
                FlowActionAttribute.ACTION_DETAILS_NAME.key(), configureDetails.getName()
            );
            case ConnectDetails connectDetails -> {
                attributes.put(FlowActionAttribute.ACTION_DETAILS_SOURCE_ID.key(), connectDetails.getSourceId());
                attributes.put(FlowActionAttribute.ACTION_DETAILS_SOURCE_TYPE.key(), connectDetails.getSourceType().name());
                attributes.put(FlowActionAttribute.ACTION_DETAILS_DESTINATION_ID.key(), connectDetails.getDestinationId());
                attributes.put(FlowActionAttribute.ACTION_DETAILS_DESTINATION_TYPE.key(), connectDetails.getDestinationType().name());
                attributes.put(FlowActionAttribute.ACTION_DETAILS_RELATIONSHIP.key(), connectDetails.getRelationship());
            }
            case MoveDetails moveDetails -> {
                attributes.put(FlowActionAttribute.ACTION_DETAILS_GROUP_ID.key(), moveDetails.getGroupId());
                attributes.put(FlowActionAttribute.ACTION_DETAILS_PREVIOUS_GROUP_ID.key(), moveDetails.getPreviousGroupId());
            }
            case PurgeDetails purgeDetails -> attributes.put(
                FlowActionAttribute.ACTION_DETAILS_END_DATE.key(), purgeDetails.getEndDate().toInstant().toString()
            );
            case null, default -> {
            }
        }
    }

    private void populateComponentDetailsProperties(final ComponentDetails componentDetails, final Map<String, String> attributes) {
        switch (componentDetails) {
            case ExtensionDetails extensionDetails -> attributes.put(
                FlowActionAttribute.COMPONENT_DETAILS_TYPE.key(), extensionDetails.getType()
            );
            case RemoteProcessGroupDetails remoteProcessGroupDetails -> attributes.put(
                FlowActionAttribute.COMPONENT_DETAILS_URI.key(), remoteProcessGroupDetails.getUri()
            );
            case null, default -> {
            }
        }
    }

    private void populateRequestDetails(final RequestDetails requestDetails, final Map<String, String> attributes) {
        if (requestDetails != null) {
            final String forwardedFor = requestDetails.getForwardedFor();
            if (forwardedFor != null) {
                attributes.put(FlowActionAttribute.REQUEST_DETAILS_FORWARDED_FOR.key(), forwardedFor);
            }

            final String remoteAddress = requestDetails.getRemoteAddress();
            if (remoteAddress != null) {
                attributes.put(FlowActionAttribute.REQUEST_DETAILS_REMOTE_ADDRESS.key(), remoteAddress);
            }

            final String userAgent = requestDetails.getUserAgent();
            if (userAgent != null) {
                attributes.put(FlowActionAttribute.REQUEST_DETAILS_USER_AGENT.key(), userAgent);
            }
        }
    }
}
