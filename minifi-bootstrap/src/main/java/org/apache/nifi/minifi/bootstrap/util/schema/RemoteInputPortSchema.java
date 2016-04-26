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

import static org.apache.nifi.minifi.bootstrap.util.schema.common.CommonPropertyKeys.COMMENT_KEY;
import static org.apache.nifi.minifi.bootstrap.util.schema.common.CommonPropertyKeys.ID_KEY;
import static org.apache.nifi.minifi.bootstrap.util.schema.common.CommonPropertyKeys.INPUT_PORTS_KEY;
import static org.apache.nifi.minifi.bootstrap.util.schema.common.CommonPropertyKeys.MAX_CONCURRENT_TASKS_KEY;
import static org.apache.nifi.minifi.bootstrap.util.schema.common.CommonPropertyKeys.NAME_KEY;
import static org.apache.nifi.minifi.bootstrap.util.schema.common.CommonPropertyKeys.USE_COMPRESSION_KEY;

/**
 *
 */
public class RemoteInputPortSchema extends BaseSchema {

    private String id;
    private String name;
    private String comment = "";
    private Number maxConcurrentTasks = 1;
    private Boolean useCompression = true;

    public RemoteInputPortSchema() {
    }

    public RemoteInputPortSchema(Map map) {
        id = getRequiredKeyAsType(map, ID_KEY, String.class, INPUT_PORTS_KEY);
        name = getRequiredKeyAsType(map, NAME_KEY, String.class, INPUT_PORTS_KEY);

        comment = getOptionalKeyAsType(map, COMMENT_KEY, String.class, INPUT_PORTS_KEY, "");
        maxConcurrentTasks = getOptionalKeyAsType(map, MAX_CONCURRENT_TASKS_KEY, Number.class, INPUT_PORTS_KEY, 1);
        useCompression = getOptionalKeyAsType(map, USE_COMPRESSION_KEY, Boolean.class, INPUT_PORTS_KEY, true);
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getComment() {
        return comment;
    }

    public Number getMax_concurrent_tasks() {
        return maxConcurrentTasks;
    }

    public boolean getUseCompression() {
        return useCompression;
    }
}
