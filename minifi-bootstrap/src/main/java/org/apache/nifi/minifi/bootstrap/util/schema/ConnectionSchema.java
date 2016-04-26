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
package org.apache.nifi.minifi.bootstrap.util.schema;

import org.apache.nifi.minifi.bootstrap.util.schema.common.BaseSchema;

import java.util.Map;

import static org.apache.nifi.minifi.bootstrap.util.schema.common.CommonPropertyKeys.CONNECTIONS_KEY;
import static org.apache.nifi.minifi.bootstrap.util.schema.common.CommonPropertyKeys.NAME_KEY;

/**
 *
 */
public class ConnectionSchema extends BaseSchema {
    public static final String SOURCE_NAME_KEY = "source name";
    public static final String SOURCE_RELATIONSHIP_NAME_KEY = "source relationship name";
    public static final String DESTINATION_NAME_KEY = "destination name";
    public static final String MAX_WORK_QUEUE_SIZE_KEY = "max work queue size";
    public static final String MAX_WORK_QUEUE_DATA_SIZE_KEY = "max work queue data size";
    public static final String FLOWFILE_EXPIRATION__KEY = "flowfile expiration";
    public static final String QUEUE_PRIORITIZER_CLASS_KEY = "queue prioritizer class";

    private String name;
    private String sourceName;
    private String sourceRelationshipName;
    private String destinationName;
    private Number maxWorkQueueSize = 0;
    private String maxWorkQueueDataSize = "0 MB";
    private String flowfileExpiration = "0 sec";
    private String queuePrioritizerClass = "";

    public ConnectionSchema() {
    }

    public ConnectionSchema(Map map) {
        name = getRequiredKeyAsType(map, NAME_KEY, String.class, CONNECTIONS_KEY);
        sourceName = getRequiredKeyAsType(map, SOURCE_NAME_KEY, String.class, CONNECTIONS_KEY);
        sourceRelationshipName = getRequiredKeyAsType(map, SOURCE_RELATIONSHIP_NAME_KEY, String.class, CONNECTIONS_KEY);
        destinationName = getRequiredKeyAsType(map, DESTINATION_NAME_KEY, String.class, CONNECTIONS_KEY);

        maxWorkQueueSize = getOptionalKeyAsType(map, MAX_WORK_QUEUE_SIZE_KEY, Number.class, CONNECTIONS_KEY, 0);
        maxWorkQueueDataSize = getOptionalKeyAsType(map, MAX_WORK_QUEUE_DATA_SIZE_KEY, String.class, CONNECTIONS_KEY, "0 MB");
        flowfileExpiration = getOptionalKeyAsType(map, FLOWFILE_EXPIRATION__KEY, String.class, CONNECTIONS_KEY, "0 sec");
        queuePrioritizerClass = getOptionalKeyAsType(map, QUEUE_PRIORITIZER_CLASS_KEY, String.class, CONNECTIONS_KEY, "");
    }

    public String getName() {
        return name;
    }

    public String getSourceName() {
        return sourceName;
    }

    public String getSourceRelationshipName() {
        return sourceRelationshipName;
    }

    public String getDestinationName() {
        return destinationName;
    }

    public Number getMaxWorkQueueSize() {
        return maxWorkQueueSize;
    }

    public String getMaxWorkQueueDataSize() {
        return maxWorkQueueDataSize;
    }

    public String getFlowfileExpiration() {
        return flowfileExpiration;
    }

    public String getQueuePrioritizerClass() {
        return queuePrioritizerClass;
    }
}
