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

import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.ALWAYS_SYNC_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.CONTENT_REPO_KEY;

/**
 *
 */
public class ContentRepositorySchema extends BaseSchema {
    public static final String CONTENT_CLAIM_MAX_APPENDABLE_SIZE_KEY = "content claim max appendable size";
    public static final String CONTENT_CLAIM_MAX_FLOW_FILES_KEY = "content claim max flow files";

    public static final String DEFAULT_CONTENT_CLAIM_MAX_APPENDABLE_SIZE = "10 MB";
    public static final int DEFAULT_CONTENT_CLAIM_MAX_FLOW_FILES = 100;
    public static final boolean DEFAULT_ALWAYS_SYNC = false;

    private String contentClaimMaxAppendableSize = DEFAULT_CONTENT_CLAIM_MAX_APPENDABLE_SIZE;
    private Number contentClaimMaxFlowFiles = DEFAULT_CONTENT_CLAIM_MAX_FLOW_FILES;
    private Boolean alwaysSync = DEFAULT_ALWAYS_SYNC;

    public ContentRepositorySchema() {
    }

    public ContentRepositorySchema(Map map) {
        contentClaimMaxAppendableSize = getOptionalKeyAsType(map, CONTENT_CLAIM_MAX_APPENDABLE_SIZE_KEY, String.class,
                CONTENT_REPO_KEY, DEFAULT_CONTENT_CLAIM_MAX_APPENDABLE_SIZE);
        contentClaimMaxFlowFiles = getOptionalKeyAsType(map, CONTENT_CLAIM_MAX_FLOW_FILES_KEY, Number.class,
                CONTENT_REPO_KEY, DEFAULT_CONTENT_CLAIM_MAX_FLOW_FILES);
        alwaysSync = getOptionalKeyAsType(map, ALWAYS_SYNC_KEY, Boolean.class, CONTENT_REPO_KEY, DEFAULT_ALWAYS_SYNC);
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> result = mapSupplier.get();
        result.put(CONTENT_CLAIM_MAX_APPENDABLE_SIZE_KEY, contentClaimMaxAppendableSize);
        result.put(CONTENT_CLAIM_MAX_FLOW_FILES_KEY, contentClaimMaxFlowFiles);
        result.put(ALWAYS_SYNC_KEY, alwaysSync);
        return result;
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
