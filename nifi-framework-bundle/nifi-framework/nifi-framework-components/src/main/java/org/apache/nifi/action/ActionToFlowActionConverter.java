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
package org.apache.nifi.action;

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
 * Converts an {@link Action} to a {@link FlowAction} using {@link FlowActionAttributes}.
 */
public class ActionToFlowActionConverter implements ActionConverter {

    @Override
    public FlowAction convert(Action action) {
        Map<String, String> attributes = new HashMap<>();
        attributes.putAll(actionAttributes(action));
        attributes.putAll(actionDetailsAttributes(action.getActionDetails()));
        attributes.putAll(componentDetailsProperties(action.getComponentDetails()));
        return new StandardFlowAction(attributes);
    }

    private Map<String, String> actionAttributes(Action action) {
        return Map.of(
                FlowActionAttributes.ACTION.ID.key(), String.valueOf(action.getId()),
                FlowActionAttributes.ACTION.TIMESTAMP.key(), action.getTimestamp().toInstant().toString(),
                FlowActionAttributes.ACTION.USER_IDENTITY.key(), action.getUserIdentity(),
                FlowActionAttributes.ACTION.SOURCE_ID.key(), action.getSourceId(),
                FlowActionAttributes.ACTION.SOURCE_TYPE.key(), action.getSourceType().name(),
                FlowActionAttributes.ACTION.OPERATION.key(), action.getOperation().name()
        );
    }

    private Map<String, String> actionDetailsAttributes(ActionDetails actionDetails) {
        return switch (actionDetails) {
            case ConfigureDetails configureDetails -> Map.of(
                    FlowActionAttributes.ACTION_DETAILS.NAME.key(), configureDetails.getName()
            );
            case ConnectDetails connectDetails -> Map.of(
                    FlowActionAttributes.ACTION_DETAILS.SOURCE_ID.key(), connectDetails.getSourceId(),
                    FlowActionAttributes.ACTION_DETAILS.SOURCE_TYPE.key(), connectDetails.getSourceType().name(),
                    FlowActionAttributes.ACTION_DETAILS.DESTINATION_ID.key(), connectDetails.getDestinationId(),
                    FlowActionAttributes.ACTION_DETAILS.DESTINATION_TYPE.key(), connectDetails.getDestinationType().name(),
                    FlowActionAttributes.ACTION_DETAILS.RELATIONSHIP.key(), connectDetails.getRelationship()
            );
            case MoveDetails moveDetails -> Map.of(
                    FlowActionAttributes.ACTION_DETAILS.GROUP_ID.key(), moveDetails.getGroupId(),
                    FlowActionAttributes.ACTION_DETAILS.PREVIOUS_GROUP_ID.key(), moveDetails.getPreviousGroupId()
            );
            case PurgeDetails purgeDetails -> Map.of(
                    FlowActionAttributes.ACTION_DETAILS.END_DATE.key(), purgeDetails.getEndDate().toInstant().toString()
            );
            case null, default -> Map.of();
        };
    }

    private Map<String, String> componentDetailsProperties(ComponentDetails componentDetails) {
        return switch (componentDetails) {
            case ExtensionDetails extensionDetails -> Map.of(
                    FlowActionAttributes.COMPONENT_DETAILS.TYPE.key(), extensionDetails.getType()

            );
            case RemoteProcessGroupDetails remoteProcessGroupDetails -> Map.of(
                    FlowActionAttributes.COMPONENT_DETAILS.URI.key(), remoteProcessGroupDetails.getUri()
            );
            case null, default -> Map.of();
        };
    }
}
