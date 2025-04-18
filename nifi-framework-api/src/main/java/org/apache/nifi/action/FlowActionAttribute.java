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

/**
 * Flow Action attributes for consistent naming
 */
public enum FlowActionAttribute {

    ACTION_ID("action.id"),
    ACTION_TIMESTAMP("action.timestamp"),
    ACTION_USER_IDENTITY("action.userIdentity"),
    ACTION_SOURCE_ID("action.sourceId"),
    ACTION_SOURCE_TYPE("action.sourceType"),
    ACTION_OPERATION("action.operation"),

    ACTION_DETAILS_NAME("actionDetails.name"),
    ACTION_DETAILS_SOURCE_ID("actionDetails.sourceId"),
    ACTION_DETAILS_SOURCE_TYPE("actionDetails.sourceType"),
    ACTION_DETAILS_DESTINATION_ID("actionDetails.destinationId"),
    ACTION_DETAILS_DESTINATION_TYPE("actionDetails.destinationType"),
    ACTION_DETAILS_RELATIONSHIP("actionDetails.relationship"),
    ACTION_DETAILS_GROUP_ID("actionDetails.groupId"),
    ACTION_DETAILS_PREVIOUS_GROUP_ID("actionDetails.previousGroupId"),
    ACTION_DETAILS_END_DATE("actionDetails.endDate"),

    COMPONENT_DETAILS_TYPE("componentDetails.type"),
    COMPONENT_DETAILS_URI("componentDetails.uri"),

    REQUEST_DETAILS_FORWARDED_FOR("requestDetails.forwardedFor"),
    REQUEST_DETAILS_REMOTE_ADDRESS("requestDetails.remoteAddress"),
    REQUEST_DETAILS_USER_AGENT("requestDetails.userAgent");

    private final String key;

    FlowActionAttribute(String key) {
        this.key = key;
    }

    public String key() {
        return key;
    }

}
