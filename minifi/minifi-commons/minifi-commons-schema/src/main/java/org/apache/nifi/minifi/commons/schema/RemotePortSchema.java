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

import org.apache.nifi.minifi.commons.schema.common.BaseSchemaWithIdAndName;

import java.util.Map;

import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.COMMENT_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.MAX_CONCURRENT_TASKS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.USE_COMPRESSION_KEY;

public class RemotePortSchema extends BaseSchemaWithIdAndName {
    public static final String DEFAULT_COMMENT = "";
    public static final int DEFAULT_MAX_CONCURRENT_TASKS = 1;
    public static final boolean DEFAULT_USE_COMPRESSION = true;

    private String comment = DEFAULT_COMMENT;
    private Number maxConcurrentTasks = DEFAULT_MAX_CONCURRENT_TASKS;
    private Boolean useCompression = DEFAULT_USE_COMPRESSION;

    public RemotePortSchema(Map map) {
        super(map, "RemoteInputPort(id: {id}, name: {name})");
        String wrapperName = getWrapperName();

        comment = getOptionalKeyAsType(map, COMMENT_KEY, String.class, wrapperName, DEFAULT_COMMENT);
        maxConcurrentTasks = getOptionalKeyAsType(map, MAX_CONCURRENT_TASKS_KEY, Number.class, wrapperName, DEFAULT_MAX_CONCURRENT_TASKS);
        useCompression = getOptionalKeyAsType(map, USE_COMPRESSION_KEY, Boolean.class, wrapperName, DEFAULT_USE_COMPRESSION);
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> result = super.toMap();
        result.put(COMMENT_KEY, comment);
        result.put(MAX_CONCURRENT_TASKS_KEY, maxConcurrentTasks);
        result.put(USE_COMPRESSION_KEY, useCompression);
        return result;
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
