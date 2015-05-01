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

/**
 * A query for bulletin board. Will filter the resulting bulletin board according to the criteria in this query.
 */
@XmlType(name = "bulletinQuery")
public class BulletinQueryDTO {

    private String sourceId;
    private String groupId;
    private String name;
    private String message;
    private Long after;
    private Integer limit;

    /**
     * @return Include bulletins after this id
     */
    @ApiModelProperty(
            value = "Will include bulletins that occurred after this id. The ids are a one-up number that are used to ensure bulletins that "
                    + "occur at the same time will not be missed."
    )
    public Long getAfter() {
        return after;
    }

    public void setAfter(Long after) {
        this.after = after;
    }

    /**
     * @return Include bulletin within this group. Supports a regular expression
     */
    @ApiModelProperty(
            value = "Will include bulletins that occurred within this group. Supports a regular expression."
    )
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    /**
     * @return Include bulletins that match this message. Supports a regular expression
     */
    @ApiModelProperty(
            value = "Will include bulletins that match this message. Supports a regular expression."
    )
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    /**
     * @return Include bulletins that match this name. Supports a regular expression
     */
    @ApiModelProperty(
            value = "Will include bulletins that match this name. Supports a regular expression."
    )
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return Include bulletins that match this id. Supports a source id
     */
    @ApiModelProperty(
            value = "Will include bulletins from components that match this id. Supports a regular expression."
    )
    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    /**
     * @return The maximum number of bulletins to return
     */
    @ApiModelProperty(
            value = "The maximum number of bulletins to return."
    )
    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

}
