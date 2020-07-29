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

package org.apache.nifi.web.api.dto.diagnostics;

import javax.xml.bind.annotation.XmlType;

import io.swagger.annotations.ApiModelProperty;

@XmlType(name = "repositoryUsage")
public class RepositoryUsageDTO implements Cloneable {
    private String name;
    private String fileStoreHash;

    private String freeSpace;
    private String totalSpace;
    private Long freeSpaceBytes;
    private Long totalSpaceBytes;
    private String utilization;


    @ApiModelProperty("The name of the repository")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @ApiModelProperty("A SHA-256 hash of the File Store name/path that is used to store the repository's data. This information is exposed as a hash in order to avoid "
        + "exposing potentially sensitive information that is not generally relevant. What is typically relevant is whether or not multiple repositories on the same node are "
        + "using the same File Store, as this indicates that the repositories are competing for the resources of the backing disk/storage mechanism.")
    public String getFileStoreHash() {
        return fileStoreHash;
    }

    public void setFileStoreHash(String fileStore) {
        this.fileStoreHash = fileStore;
    }

    @ApiModelProperty("Amount of free space.")
    public String getFreeSpace() {
        return freeSpace;
    }

    public void setFreeSpace(String freeSpace) {
        this.freeSpace = freeSpace;
    }

    @ApiModelProperty("Amount of total space.")
    public String getTotalSpace() {
        return totalSpace;
    }

    public void setTotalSpace(String totalSpace) {
        this.totalSpace = totalSpace;
    }

    @ApiModelProperty("Utilization of this storage location.")
    public String getUtilization() {
        return utilization;
    }

    public void setUtilization(String utilization) {
        this.utilization = utilization;
    }

    @ApiModelProperty("The number of bytes of free space.")
    public Long getFreeSpaceBytes() {
        return freeSpaceBytes;
    }

    public void setFreeSpaceBytes(Long freeSpaceBytes) {
        this.freeSpaceBytes = freeSpaceBytes;
    }

    @ApiModelProperty("The number of bytes of total space.")
    public Long getTotalSpaceBytes() {
        return totalSpaceBytes;
    }

    public void setTotalSpaceBytes(Long totalSpaceBytes) {
        this.totalSpaceBytes = totalSpaceBytes;
    }

    @Override
    public RepositoryUsageDTO clone() {
        final RepositoryUsageDTO clone = new RepositoryUsageDTO();
        clone.fileStoreHash = fileStoreHash;
        clone.freeSpace = freeSpace;
        clone.freeSpaceBytes = freeSpaceBytes;
        clone.name = name;
        clone.totalSpace = totalSpace;
        clone.totalSpaceBytes = totalSpaceBytes;
        clone.utilization = utilization;
        return clone;
    }
}
