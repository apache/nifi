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

import java.util.Map;
import javax.xml.bind.annotation.XmlType;

/**
 * History of a processor's properties.
 */
@XmlType(name = "processorHistory")
public class ProcessorHistoryDTO {

    private String processorId;
    private Map<String, PropertyHistoryDTO> propertyHistory;

    /**
     * The processor id.
     *
     * @return
     */
    public String getProcessorId() {
        return processorId;
    }

    public void setProcessorId(String processorId) {
        this.processorId = processorId;
    }

    /**
     * The history for this processors properties.
     *
     * @return
     */
    public Map<String, PropertyHistoryDTO> getPropertyHistory() {
        return propertyHistory;
    }

    public void setPropertyHistory(Map<String, PropertyHistoryDTO> propertyHistory) {
        this.propertyHistory = propertyHistory;
    }
}
