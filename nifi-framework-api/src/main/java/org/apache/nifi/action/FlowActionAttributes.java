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
public interface FlowActionAttributes {

    enum ACTION implements FlowActionAttribute {
        ID("id"),
        TIMESTAMP("timestamp"),
        USER_IDENTITY("userIdentity"),
        SOURCE_ID("sourceId"),
        SOURCE_TYPE("sourceType"),
        OPERATION("operation");

        private static final String PREFIX = "action.";
        private final String key;

        ACTION(String key) {
            this.key = key;
        }

        @Override
        public String key() {
            return PREFIX + key;
        }
    }

    enum ACTION_DETAILS implements FlowActionAttribute {
        NAME("name"),
        SOURCE_ID("sourceId"),
        SOURCE_TYPE("sourceType"),
        DESTINATION_ID("destinationId"),
        DESTINATION_TYPE("destinationType"),
        RELATIONSHIP("relationship"),
        GROUP_ID("groupId"),
        PREVIOUS_GROUP_ID("previousGroupId"),
        END_DATE("endDate");

        private static final String PREFIX = "actionDetails.";
        private final String key;

        ACTION_DETAILS(String key) {
            this.key = key;
        }

        @Override
        public String key() {
            return PREFIX + key;
        }
    }

    enum COMPONENT_DETAILS implements FlowActionAttribute {
        TYPE("type"),
        URI("uri");

        private static final String PREFIX = "componentDetails.";
        private final String key;

        COMPONENT_DETAILS(String key) {
            this.key = key;
        }

        @Override
        public String key() {
            return PREFIX + key;
        }
    }

}
