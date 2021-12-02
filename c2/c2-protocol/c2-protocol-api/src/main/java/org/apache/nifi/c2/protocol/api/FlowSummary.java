/*
 * Apache NiFi
 * Copyright 2014-2018 The Apache Software Foundation
 *
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

package org.apache.nifi.c2.protocol.api;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.Serializable;
import java.net.URI;

@ApiModel
public class FlowSummary implements Serializable {
    private static final long serialVersionUID = 4788329218675299184L;

    private String id;

    private FlowFormat flowFormat;

    private Long createdTime;

    private Long updatedTime;

    private URI uri;

    @ApiModelProperty(value = "A unique identifier of the flow", required = true)
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @ApiModelProperty(value = "The format of the flow indicating how the content should be interpreted when retrieving the flow content")
    public FlowFormat getFlowFormat() {
        return flowFormat;
    }

    public void setFlowFormat(FlowFormat flowFormat) {
        this.flowFormat = flowFormat;
    }

    @ApiModelProperty(value = "A timestamp (ms since epoch) for when this flow was created in the C2 server")
    public Long getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(Long createdTime) {
        this.createdTime = createdTime;
    }

    @ApiModelProperty(value = "A timestamp (ms since epoch) for when this flow was updated in the C2 server")
    public Long getUpdatedTime() {
        return updatedTime;
    }

    public void setUpdatedTime(Long updatedTime) {
        this.updatedTime = updatedTime;
    }

    @ApiModelProperty(value = "The Uniform Resource Identifier (URI) for this flow")
    public URI getUri() {
        return uri;
    }

    public void setUri(URI uri) {
        this.uri = uri;
    }
}
