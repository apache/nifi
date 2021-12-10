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

package org.apache.nifi.c2.protocol.api;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;

@ApiModel
public class AgentRepositoryStatus implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long size;
    private Long sizeMax;
    private Long dataSize;
    private Long dataSizeMax;

    @ApiModelProperty(value = "The number of items in the repository", allowableValues = "range[0, 9223372036854775807]")
    public Long getSize() {
        return size;
    }

    public void setSize(Long size) {
        this.size = size;
    }

    @ApiModelProperty(value = "The maximum number of items the repository is capable of storing", allowableValues = "range[0, 9223372036854775807]")
    public Long getSizeMax() {
        return sizeMax;
    }

    public void setSizeMax(Long sizeMax) {
        this.sizeMax = sizeMax;
    }

    @ApiModelProperty(value = "The data size (in Bytes) of all items in the repository", allowableValues = "range[0, 9223372036854775807]")
    public Long getDataSize() {
        return dataSize;
    }

    public void setDataSize(Long dataSize) {
        this.dataSize = dataSize;
    }

    @ApiModelProperty(value = "The maximum data size (in Bytes) that the repository is capable of storing", allowableValues = "range[0, 9223372036854775807]")
    public Long getDataSizeMax() {
        return dataSizeMax;
    }

    public void setDataSizeMax(Long dataSizeMax) {
        this.dataSizeMax = dataSizeMax;
    }

    /**
     * If sizeMax is set, returns a decimal between [0, 1] indicating the ratio
     * of size to sizeMax.
     * <p>
     * If size or sizeMax are null, this method return null.
     *
     * @return a decimal between [0, 1] representing the sizeMax utilization percentage
     */
    @ApiModelProperty(hidden = true)
    public Double getSizeUtilization() {
        return size != null && sizeMax != null && sizeMax > 0 ? (double) size / (double) sizeMax : null;
    }

    /**
     * If dataSizeMax is set, returns a decimal between [0, 1] indicating the ratio
     * of dataSize to dataSizeMax.
     *
     * @return a decimal between [0, 1] representing the dataSizeMax utilization percentage
     */
    @ApiModelProperty(hidden = true)
    public Double getDataSizeUtilization() {
        return dataSize != null && dataSizeMax != null && dataSizeMax > 0 ? (double) dataSize / (double) dataSizeMax : null;
    }
}
