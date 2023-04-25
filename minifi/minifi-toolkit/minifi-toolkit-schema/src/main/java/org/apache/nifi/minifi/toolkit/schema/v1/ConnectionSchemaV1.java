/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.nifi.minifi.toolkit.schema.v1;

import org.apache.nifi.minifi.toolkit.schema.ConnectionSchema;
import org.apache.nifi.minifi.toolkit.schema.common.BaseSchema;
import org.apache.nifi.minifi.toolkit.schema.common.ConvertableSchema;
import org.apache.nifi.minifi.toolkit.schema.common.StringUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.minifi.toolkit.schema.ConnectionSchema.DEFAULT_FLOWFILE_EXPIRATION;
import static org.apache.nifi.minifi.toolkit.schema.ConnectionSchema.DEFAULT_MAX_QUEUE_DATA_SIZE;
import static org.apache.nifi.minifi.toolkit.schema.ConnectionSchema.DEFAULT_MAX_WORK_QUEUE_SIZE;
import static org.apache.nifi.minifi.toolkit.schema.ConnectionSchema.FLOWFILE_EXPIRATION__KEY;
import static org.apache.nifi.minifi.toolkit.schema.ConnectionSchema.MAX_WORK_QUEUE_DATA_SIZE_KEY;
import static org.apache.nifi.minifi.toolkit.schema.ConnectionSchema.MAX_WORK_QUEUE_SIZE_KEY;
import static org.apache.nifi.minifi.toolkit.schema.ConnectionSchema.QUEUE_PRIORITIZER_CLASS_KEY;
import static org.apache.nifi.minifi.toolkit.schema.ConnectionSchema.SOURCE_RELATIONSHIP_NAMES_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.CONNECTIONS_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.NAME_KEY;

public class ConnectionSchemaV1 extends BaseSchema implements ConvertableSchema<ConnectionSchema> {
    public static final String SOURCE_RELATIONSHIP_NAME_KEY = "source relationship name";
    public static final String DESTINATION_NAME_KEY = "destination name";
    public static final String SOURCE_NAME_KEY = "source name";

    private String name;

    private String sourceRelationshipName;
    private String destinationName;

    private String sourceName;

    private Number maxWorkQueueSize = DEFAULT_MAX_WORK_QUEUE_SIZE;
    private String maxWorkQueueDataSize = DEFAULT_MAX_QUEUE_DATA_SIZE;
    private String flowfileExpiration = DEFAULT_FLOWFILE_EXPIRATION;
    private String queuePrioritizerClass;

    public ConnectionSchemaV1(Map map) {
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
    public ConnectionSchema convert() {
        Map<String, Object> map = new HashMap<>();
        map.put(NAME_KEY, name);
        if (StringUtil.isNullOrEmpty(sourceRelationshipName)) {
            map.put(SOURCE_RELATIONSHIP_NAMES_KEY, new ArrayList<>());
        } else {
            map.put(SOURCE_RELATIONSHIP_NAMES_KEY, new ArrayList<>(Arrays.asList(sourceRelationshipName)));
        }
        map.put(MAX_WORK_QUEUE_SIZE_KEY, maxWorkQueueSize);
        map.put(MAX_WORK_QUEUE_DATA_SIZE_KEY, maxWorkQueueDataSize);
        map.put(FLOWFILE_EXPIRATION__KEY, flowfileExpiration);
        map.put(QUEUE_PRIORITIZER_CLASS_KEY, queuePrioritizerClass);
        return new ConnectionSchema(map);
    }

    public String getSourceName() {
        return sourceName;
    }

    public String getDestinationName() {
        return destinationName;
    }

    public String getName() {
        return name;
    }

    @Override
    public int getVersion() {
        return ConfigSchemaV1.CONFIG_VERSION;
    }
}
