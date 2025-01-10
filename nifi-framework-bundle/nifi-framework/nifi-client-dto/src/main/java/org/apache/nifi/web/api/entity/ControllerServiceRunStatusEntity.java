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
package org.apache.nifi.web.api.entity;

import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.xml.bind.annotation.XmlType;

/**
 * Run status for a given ControllerService.
 */
@XmlType(name = "controllerServiceRunStatus")
public class ControllerServiceRunStatusEntity extends ComponentRunStatusEntity {

    private static final String[] SUPPORTED_STATE = {"ENABLED", "DISABLED"};
    private boolean uiOnly;

    @Override
    protected String[] getSupportedState() {
        return SUPPORTED_STATE;
    }

    /**
     * Run status for this ControllerService.
     * @return The run status
     */
    @Schema(description = "The run status of the ControllerService.",
            allowableValues = {"ENABLED", "DISABLED"}
    )
    public String getState() {
        return super.getState();
    }

    @Schema(description = """
            Indicates whether or not responses should only include fields necessary for rendering the NiFi User Interface.
            As such, when this value is set to true, some fields may be returned as null values, and the selected fields may change at any time without notice.
            As a result, this value should not be set to true by any client other than the UI.
            """
    )
    public Boolean getUiOnly() {
        return uiOnly;
    }

    public void setUiOnly(final Boolean uiOnly) {
        this.uiOnly = uiOnly;
    }

}
