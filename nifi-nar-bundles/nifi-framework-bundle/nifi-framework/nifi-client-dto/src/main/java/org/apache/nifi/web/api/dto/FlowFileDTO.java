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
package org.apache.nifi.web.api.dto;

import com.wordnik.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlType;
import java.util.Map;

@XmlType(name = "flowFile")
public class FlowFileDTO extends FlowFileSummaryDTO {

    private Map<String, String> attributes;

    private String contentClaimSection;
    private String contentClaimContainer;
    private String contentClaimIdentifier;
    private Long contentClaimOffset;
    private String contentClaimFileSize;
    private Long contentClaimFileSizeBytes;

    /**
     * @return the FlowFile attributes
     */
    @ApiModelProperty(
        value = "The FlowFile attributes."
    )
    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    /**
     * @return the Section in which the Content Claim lives, or <code>null</code> if no Content Claim exists
     */
    @ApiModelProperty(
        value = "The section in which the content claim lives."
    )
    public String getContentClaimSection() {
        return contentClaimSection;
    }

    public void setContentClaimSection(String contentClaimSection) {
        this.contentClaimSection = contentClaimSection;
    }

    /**
     * @return the Container in which the Content Claim lives, or <code>null</code> if no Content Claim exists
     */
    @ApiModelProperty(
        value = "The container in which the content claim lives."
    )
    public String getContentClaimContainer() {
        return contentClaimContainer;
    }

    public void setContentClaimContainer(String contentClaimContainer) {
        this.contentClaimContainer = contentClaimContainer;
    }

    /**
     * @return the Identifier of the Content Claim, or <code>null</code> if no Content Claim exists
     */
    @ApiModelProperty(
        value = "The identifier of the content claim."
    )
    public String getContentClaimIdentifier() {
        return contentClaimIdentifier;
    }

    public void setContentClaimIdentifier(String contentClaimIdentifier) {
        this.contentClaimIdentifier = contentClaimIdentifier;
    }

    /**
     * @return the offset into the the Content Claim where the FlowFile's content begins, or <code>null</code> if no Content Claim exists
     */
    @ApiModelProperty(
        value = "The offset into the content claim where the flowfile's content begins."
    )
    public Long getContentClaimOffset() {
        return contentClaimOffset;
    }

    public void setContentClaimOffset(Long contentClaimOffset) {
        this.contentClaimOffset = contentClaimOffset;
    }

    /**
     * @return the formatted file size of the content claim
     */
    @ApiModelProperty(
        value = "The file size of the content claim formatted."
    )
    public String getContentClaimFileSize() {
        return contentClaimFileSize;
    }

    public void setContentClaimFileSize(String contentClaimFileSize) {
        this.contentClaimFileSize = contentClaimFileSize;
    }

    /**
     * @return the number of bytes of the content claim
     */
    @ApiModelProperty(
        value = "The file size of the content claim in bytes."
    )
    public Long getContentClaimFileSizeBytes() {
        return contentClaimFileSizeBytes;
    }

    public void setContentClaimFileSizeBytes(Long contentClaimFileSizeBytes) {
        this.contentClaimFileSizeBytes = contentClaimFileSizeBytes;
    }
}
