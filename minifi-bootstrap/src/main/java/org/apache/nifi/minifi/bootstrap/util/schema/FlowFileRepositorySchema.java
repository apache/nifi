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

import static org.apache.nifi.minifi.bootstrap.util.schema.common.CommonPropertyKeys.ALWAYS_SYNC_KEY;
import static org.apache.nifi.minifi.bootstrap.util.schema.common.CommonPropertyKeys.FLOWFILE_REPO_KEY;
import static org.apache.nifi.minifi.bootstrap.util.schema.common.CommonPropertyKeys.SWAP_PROPS_KEY;

/**
 *
 */
public class FlowFileRepositorySchema extends BaseSchema {

    public static final String PARTITIONS_KEY = "partitions";
    public static final String CHECKPOINT_INTERVAL_KEY = "checkpoint interval";

    private Number partitions = 256;
    private String checkpointInterval = "2 mins";
    private Boolean alwaysSync = false;
    private SwapSchema swapProperties;

    public FlowFileRepositorySchema() {
        swapProperties = new SwapSchema();
    }

    public FlowFileRepositorySchema(Map map) {
        partitions = getOptionalKeyAsType(map, PARTITIONS_KEY, Number.class, FLOWFILE_REPO_KEY, 256);
        checkpointInterval = getOptionalKeyAsType(map, CHECKPOINT_INTERVAL_KEY, String.class, FLOWFILE_REPO_KEY, "2 mins");
        alwaysSync = getOptionalKeyAsType(map, ALWAYS_SYNC_KEY, Boolean.class, FLOWFILE_REPO_KEY, false);

        swapProperties = getMapAsType(map, SWAP_PROPS_KEY, SwapSchema.class, FLOWFILE_REPO_KEY, false);
        addIssuesIfNotNull(swapProperties);
    }

    public Number getPartitions() {
        return partitions;
    }

    public String getCheckpointInterval() {
        return checkpointInterval;
    }

    public boolean getAlwaysSync() {
        return alwaysSync;
    }

    public SwapSchema getSwapProperties() {
        return swapProperties;
    }
}
