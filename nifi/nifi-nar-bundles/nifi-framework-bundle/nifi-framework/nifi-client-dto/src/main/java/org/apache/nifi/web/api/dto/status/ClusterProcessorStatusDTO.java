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

import java.util.Collection;
import java.util.Date;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.nifi.web.api.dto.util.TimeAdapter;

/**
 * DTO for serializing the a processor's status across the cluster.
 */
@XmlType(name = "clusterProcessorStatus")
public class ClusterProcessorStatusDTO {

    private Collection<NodeProcessorStatusDTO> nodeProcessorStatus;
    private Date statsLastRefreshed;
    private String processorId;
    private String processorName;
    private String processorType;
    private String processorRunStatus;

    /**
     * @return time the status were last refreshed
     */
    @XmlJavaTypeAdapter(TimeAdapter.class)
    public Date getStatsLastRefreshed() {
        return statsLastRefreshed;
    }

    public void setStatsLastRefreshed(Date statsLastRefreshed) {
        this.statsLastRefreshed = statsLastRefreshed;
    }

    /**
     * @return processor id
     */
    public String getProcessorId() {
        return processorId;
    }

    public void setProcessorId(String processorId) {
        this.processorId = processorId;
    }

    /**
     * @return processor name
     */
    public String getProcessorName() {
        return processorName;
    }

    public void setProcessorName(String processorName) {
        this.processorName = processorName;
    }

    /**
     * @return processor type
     */
    public String getProcessorType() {
        return processorType;
    }

    public void setProcessorType(String processorType) {
        this.processorType = processorType;
    }

    /**
     * @return processor run status
     */
    public String getProcessorRunStatus() {
        return processorRunStatus;
    }

    public void setProcessorRunStatus(String runStatus) {
        this.processorRunStatus = runStatus;
    }

    /**
     * Collection of node processor status DTO.
     *
     * @return The collection of node processor status DTO
     */
    public Collection<NodeProcessorStatusDTO> getNodeProcessorStatus() {
        return nodeProcessorStatus;
    }

    public void setNodeProcessorStatus(Collection<NodeProcessorStatusDTO> nodeProcessorStatus) {
        this.nodeProcessorStatus = nodeProcessorStatus;
    }

}
