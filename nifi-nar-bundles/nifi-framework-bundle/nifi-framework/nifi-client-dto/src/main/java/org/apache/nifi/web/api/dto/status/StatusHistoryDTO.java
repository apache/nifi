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

import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.nifi.web.api.dto.util.TimeAdapter;

import jakarta.xml.bind.annotation.XmlType;
import jakarta.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * DTO for serializing the status history of a single component across the cluster.
 */
@XmlType(name = "statusHistory")
public class StatusHistoryDTO {

    private Date generated;

    private LinkedHashMap<String, String> componentDetails;
    private List<StatusDescriptorDTO> fieldDescriptors;
    private List<StatusSnapshotDTO> aggregateSnapshots;
    private List<NodeStatusSnapshotsDTO> nodeSnapshots;

    /**
     * @return when this status history was generated
     */
    @XmlJavaTypeAdapter(TimeAdapter.class)
    @Schema(description = "When the status history was generated.",
            type = "string"
    )
    public Date getGenerated() {
        return generated;
    }

    public void setGenerated(Date generated) {
        this.generated = generated;
    }

    /**
     * @return key/value pairs that describe the component that the status history belongs to
     */
    @Schema(description = "A Map of key/value pairs that describe the component that the status history belongs to")
    public LinkedHashMap<String, String> getComponentDetails() {
        return componentDetails;
    }

    public void setComponentDetails(LinkedHashMap<String, String> componentDetails) {
        this.componentDetails = componentDetails;
    }

    @Schema(description = "The Descriptors that provide information on each of the metrics provided in the status history")
    public List<StatusDescriptorDTO> getFieldDescriptors() {
        return fieldDescriptors;
    }

    public void setFieldDescriptors(List<StatusDescriptorDTO> fieldDescriptors) {
        this.fieldDescriptors = fieldDescriptors;
    }

    @Schema(description = "A list of StatusSnapshotDTO objects that provide the actual metric values for the component. If the NiFi instance "
        + "is clustered, this will represent the aggregate status across all nodes. If the NiFi instance is not clustered, this will represent "
        + "the status of the entire NiFi instance.")
    public List<StatusSnapshotDTO> getAggregateSnapshots() {
        return aggregateSnapshots;
    }

    public void setAggregateSnapshots(List<StatusSnapshotDTO> aggregateSnapshots) {
        this.aggregateSnapshots = aggregateSnapshots;
    }

    @Schema(description = "The NodeStatusSnapshotsDTO objects that provide the actual metric values for the component, for each node. "
        + "If the NiFi instance is not clustered, this value will be null.")
    public List<NodeStatusSnapshotsDTO> getNodeSnapshots() {
        return nodeSnapshots;
    }

    public void setNodeSnapshots(List<NodeStatusSnapshotsDTO> nodeSnapshots) {
        this.nodeSnapshots = nodeSnapshots;
    }
}
