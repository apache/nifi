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

package org.apache.nifi.processors.elasticsearch.put;

import com.jayway.jsonpath.JsonPath;

public class FlowFileJsonDescription {
    private String index;
    private String type;
    private String id;
    private String content;

    private JsonPath idJsonPath;
    private JsonPath typeJsonPath;
    private JsonPath indexJsonPath;

    public FlowFileJsonDescription(String index, String type, String id, String content, JsonPath idJsonPath, JsonPath typeJsonPath, JsonPath indexJsonPath) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.content = content;
        this.idJsonPath = idJsonPath;
        this.typeJsonPath = typeJsonPath;
        this.indexJsonPath = indexJsonPath;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    public String getContent() {
        return content;
    }

    public JsonPath getIdJsonPath() {
        return idJsonPath;
    }

    public JsonPath getTypeJsonPath() {
        return typeJsonPath;
    }

    public JsonPath getIndexJsonPath() {
        return indexJsonPath;
    }
}
