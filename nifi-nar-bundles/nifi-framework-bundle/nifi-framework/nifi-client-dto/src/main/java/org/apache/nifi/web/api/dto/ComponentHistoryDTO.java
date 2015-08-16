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
import java.util.Map;
import javax.xml.bind.annotation.XmlType;

/**
 * History of a component's properties.
 */
@XmlType(name = "componentHistory")
public class ComponentHistoryDTO {

    private String componentId;
    private Map<String, PropertyHistoryDTO> propertyHistory;

    /**
     * @return component id
     */
    @ApiModelProperty(
            value = "The component id."
    )
    public String getComponentId() {
        return componentId;
    }

    public void setComponentId(String componentId) {
        this.componentId = componentId;
    }

    /**
     * @return history for this components properties
     */
    @ApiModelProperty(
            value = "The history for the properties of the component."
    )
    public Map<String, PropertyHistoryDTO> getPropertyHistory() {
        return propertyHistory;
    }

    public void setPropertyHistory(Map<String, PropertyHistoryDTO> propertyHistory) {
        this.propertyHistory = propertyHistory;
    }
}
