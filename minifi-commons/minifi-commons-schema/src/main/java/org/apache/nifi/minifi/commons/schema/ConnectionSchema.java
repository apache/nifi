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

package org.apache.nifi.minifi.commons.schema;

import org.apache.nifi.minifi.commons.schema.common.BaseSchema;

import java.util.Map;

import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.CONNECTIONS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.NAME_KEY;

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

    public static final long DEFAULT_MAX_WORK_QUEUE_SIZE = 0;
    public static final String DEFAULT_MAX_QUEUE_DATA_SIZE = "0 MB";
    public static final String DEFAULT_FLOWFILE_EXPIRATION = "0 sec";

    private String name;
    private String sourceName;
    private String sourceRelationshipName;
    private String destinationName;

    private Number maxWorkQueueSize = DEFAULT_MAX_WORK_QUEUE_SIZE;
    private String maxWorkQueueDataSize = DEFAULT_MAX_QUEUE_DATA_SIZE;
    private String flowfileExpiration = DEFAULT_FLOWFILE_EXPIRATION;
    private String queuePrioritizerClass;

    public ConnectionSchema(Map map) {
        name = getRequiredKeyAsType(map, NAME_KEY, String.class, CONNECTIONS_KEY);
        sourceName = getRequiredKeyAsType(map, SOURCE_NAME_KEY, String.class, CONNECTIONS_KEY);
        sourceRelationshipName = getRequiredKeyAsType(map, SOURCE_RELATIONSHIP_NAME_KEY, String.class, CONNECTIONS_KEY);
        destinationName = getRequiredKeyAsType(map, DESTINATION_NAME_KEY, String.class, CONNECTIONS_KEY);

        maxWorkQueueSize = getOptionalKeyAsType(map, MAX_WORK_QUEUE_SIZE_KEY, Number.class, CONNECTIONS_KEY, DEFAULT_MAX_WORK_QUEUE_SIZE);
        maxWorkQueueDataSize = getOptionalKeyAsType(map, MAX_WORK_QUEUE_DATA_SIZE_KEY, String.class, CONNECTIONS_KEY, DEFAULT_MAX_QUEUE_DATA_SIZE);
        flowfileExpiration = getOptionalKeyAsType(map, FLOWFILE_EXPIRATION__KEY, String.class, CONNECTIONS_KEY, DEFAULT_FLOWFILE_EXPIRATION);
        queuePrioritizerClass = getOptionalKeyAsType(map, QUEUE_PRIORITIZER_CLASS_KEY, String.class, CONNECTIONS_KEY, "");
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> result = mapSupplier.get();
        result.put(NAME_KEY, name);
        result.put(SOURCE_NAME_KEY, sourceName);
        result.put(SOURCE_RELATIONSHIP_NAME_KEY, sourceRelationshipName);
        result.put(DESTINATION_NAME_KEY, destinationName);

        result.put(MAX_WORK_QUEUE_SIZE_KEY, maxWorkQueueSize);
        result.put(MAX_WORK_QUEUE_DATA_SIZE_KEY, maxWorkQueueDataSize);
        result.put(FLOWFILE_EXPIRATION__KEY, flowfileExpiration);
        result.put(QUEUE_PRIORITIZER_CLASS_KEY, queuePrioritizerClass);
        return result;
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
