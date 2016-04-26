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
import static org.apache.nifi.minifi.bootstrap.util.schema.common.CommonPropertyKeys.CONTENT_REPO_KEY;

/**
 *
 */
public class ContentRepositorySchema extends BaseSchema {
    public static final String CONTENT_CLAIM_MAX_APPENDABLE_SIZE_KEY = "content claim max appendable size";
    public static final String CONTENT_CLAIM_MAX_FLOW_FILES_KEY = "content claim max flow files";

    private String contentClaimMaxAppendableSize = "10 MB";
    private Number contentClaimMaxFlowFiles = 100;
    private Boolean alwaysSync = false;

    public ContentRepositorySchema() {
    }

    public ContentRepositorySchema(Map map) {
        contentClaimMaxAppendableSize = getOptionalKeyAsType(map, CONTENT_CLAIM_MAX_APPENDABLE_SIZE_KEY, String.class, CONTENT_REPO_KEY, "10 MB");
        contentClaimMaxFlowFiles = getOptionalKeyAsType(map, CONTENT_CLAIM_MAX_FLOW_FILES_KEY, Number.class, CONTENT_REPO_KEY, 100);
        alwaysSync = getOptionalKeyAsType(map, ALWAYS_SYNC_KEY, Boolean.class, CONTENT_REPO_KEY, false);
    }

    public String getContentClaimMaxAppendableSize() {
        return contentClaimMaxAppendableSize;
    }

    public Number getContentClaimMaxFlowFiles() {
        return contentClaimMaxFlowFiles;
    }

    public boolean getAlwaysSync() {
        return alwaysSync;
    }
}
