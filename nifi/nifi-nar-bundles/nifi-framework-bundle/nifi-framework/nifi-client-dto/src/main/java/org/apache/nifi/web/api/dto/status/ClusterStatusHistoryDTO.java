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
import java.util.Date;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.nifi.web.api.dto.util.TimeAdapter;

/**
 * DTO for serializing the a status history across the cluster.
 */
@XmlType(name = "clusterStatusHistory")
public class ClusterStatusHistoryDTO {

    private Collection<NodeStatusHistoryDTO> nodeStatusHistory;
    private StatusHistoryDTO clusterStatusHistory;
    private Date generated;

    /**
     * @return when this status history was generated
     */
    @XmlJavaTypeAdapter(TimeAdapter.class)
    @ApiModelProperty(
            value = "When the status history was generated."
    )
    public Date getGenerated() {
        return generated;
    }

    public void setGenerated(Date generated) {
        this.generated = generated;
    }

    /**
     * @return status history from each node in the cluster
     */
    @ApiModelProperty(
            value = "The status history from each node."
    )
    public Collection<NodeStatusHistoryDTO> getNodeStatusHistory() {
        return nodeStatusHistory;
    }

    public void setNodeStatusHistory(Collection<NodeStatusHistoryDTO> nodeStatusHistory) {
        this.nodeStatusHistory = nodeStatusHistory;
    }

    /**
     * @return status history for this component across the entire cluster
     */
    @ApiModelProperty(
            value = "The status history for the entire cluster."
    )
    public StatusHistoryDTO getClusterStatusHistory() {
        return clusterStatusHistory;
    }

    public void setClusterStatusHistory(StatusHistoryDTO clusterStatusHistory) {
        this.clusterStatusHistory = clusterStatusHistory;
    }

}
