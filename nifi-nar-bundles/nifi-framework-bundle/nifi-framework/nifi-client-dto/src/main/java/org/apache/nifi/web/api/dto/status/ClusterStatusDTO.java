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
package org.apache.nifi.web.api.dto.status;

import com.wordnik.swagger.annotations.ApiModelProperty;
import java.util.Collection;

import javax.xml.bind.annotation.XmlType;

/**
 * DTO for serializing the controller status.
 */
@XmlType(name = "clusterStatus")
public class ClusterStatusDTO {

    private Collection<NodeStatusDTO> nodeStatus;

    /**
     * @return collection of the node status DTOs
     */
    @ApiModelProperty(
            value = "The status of each node."
    )
    public Collection<NodeStatusDTO> getNodeStatus() {
        return nodeStatus;
    }

    public void setNodeStatus(Collection<NodeStatusDTO> nodeStatus) {
        this.nodeStatus = nodeStatus;
    }

}
