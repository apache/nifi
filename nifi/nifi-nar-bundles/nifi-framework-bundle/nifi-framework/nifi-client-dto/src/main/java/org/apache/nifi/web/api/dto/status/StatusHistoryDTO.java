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

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.nifi.web.api.dto.util.TimeAdapter;

/**
 * History status for a component in this NiFi.
 */
@XmlType(name = "statusHistory")
public class StatusHistoryDTO {

    private Date generated;

    private LinkedHashMap<String, String> details;

    private List<StatusDescriptorDTO> fieldDescriptors;
    private List<StatusSnapshotDTO> statusSnapshots;

    /**
     * @return when this status history was generated
     */
    @XmlJavaTypeAdapter(TimeAdapter.class)
    public Date getGenerated() {
        return generated;
    }

    public void setGenerated(Date generated) {
        this.generated = generated;
    }

    /**
     * @return The component details for this status history
     */
    public LinkedHashMap<String, String> getDetails() {
        return details;
    }

    public void setDetails(LinkedHashMap<String, String> details) {
        this.details = details;
    }

    /**
     * @return Descriptors for each supported status field
     */
    public List<StatusDescriptorDTO> getFieldDescriptors() {
        return fieldDescriptors;
    }

    public void setFieldDescriptors(List<StatusDescriptorDTO> fieldDescriptors) {
        this.fieldDescriptors = fieldDescriptors;
    }

    /**
     * @return The status snapshots
     */
    public List<StatusSnapshotDTO> getStatusSnapshots() {
        return statusSnapshots;
    }

    public void setStatusSnapshots(List<StatusSnapshotDTO> statusSnapshots) {
        this.statusSnapshots = statusSnapshots;
    }

}
