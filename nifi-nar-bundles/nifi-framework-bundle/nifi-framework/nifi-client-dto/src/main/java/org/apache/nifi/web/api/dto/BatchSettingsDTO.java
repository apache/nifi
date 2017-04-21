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
 * Details of batch settings of a remote process group port.
 */
@XmlType(name = "batchSettings")
public class BatchSettingsDTO {

    private Integer count;
    private String size;
    private String duration;

    /**
     * @return preferred number of flow files to include in a transaction
     */
    @ApiModelProperty(
            value = "Preferred number of flow files to include in a transaction."
    )
    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    /**
     * @return preferred number of bytes to include in a transaction
     */
    @ApiModelProperty(
            value = "Preferred number of bytes to include in a transaction."
    )
    public String getSize() {
        return size;
    }

    public void setSize(String size) {
        this.size = size;
    }

    /**
     * @return preferred amount of time that a transaction should span
     */
    @ApiModelProperty(
            value = "Preferred amount of time that a transaction should span."
    )
    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }


}
