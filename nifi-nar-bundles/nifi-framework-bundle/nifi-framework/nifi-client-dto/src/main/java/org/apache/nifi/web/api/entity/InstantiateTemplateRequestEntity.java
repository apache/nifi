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
package org.apache.nifi.web.api.entity;

import com.wordnik.swagger.annotations.ApiModelProperty;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * A serialized representation of this class can be placed in the entity body of a request to the API.
 */
@XmlRootElement(name = "instantiateTemplateRequestEntity")
public class InstantiateTemplateRequestEntity extends Entity {

    private Double originX;
    private Double originY;

    private String templateId;
    private String encodingVersion;
    private FlowSnippetDTO snippet;

    @ApiModelProperty(
            value = "The identifier of the template."
    )
    public String getTemplateId() {
        return templateId;
    }

    public void setTemplateId(String templateId) {
        this.templateId = templateId;
    }

    @ApiModelProperty(
            value = "The x coordinate of the origin of the bounding box where the new components will be placed."
    )
    public Double getOriginX() {
        return originX;
    }

    public void setOriginX(Double originX) {
        this.originX = originX;
    }

    @ApiModelProperty(
            value = "The y coordinate of the origin of the bounding box where the new components will be placed."
    )
    public Double getOriginY() {
        return originY;
    }

    public void setOriginY(Double originY) {
        this.originY = originY;
    }

    @ApiModelProperty(
            value = "The encoding version of the flow snippet. If not specified, this is automatically "
                    + "populated by the node receiving the user request. If the snippet is specified, the version "
                    + "will be the latest. If the snippet is not specified, the version will come from the underlying "
                    + "template. These details need to be replicated throughout the cluster to ensure consistency."
    )
    public String getEncodingVersion() {
        return encodingVersion;
    }

    public void setEncodingVersion(String encodingVersion) {
        this.encodingVersion = encodingVersion;
    }

    @ApiModelProperty(
            value = "A flow snippet of the template contents. If not specified, this is automatically "
                    + "populated by the node receiving the user request. These details need to be replicated "
                    + "throughout the cluster to ensure consistency."
    )
    public FlowSnippetDTO getSnippet() {
        return snippet;
    }

    public void setSnippet(FlowSnippetDTO snippet) {
        this.snippet = snippet;
    }
}
