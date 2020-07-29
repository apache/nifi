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

import io.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlType;

/**
 * Run status for a given ReportingTask.
 */
@XmlType(name = "reportingTaskRunStatus")
public class ReportingTaskRunStatusEntity extends ComponentRunStatusEntity {

    private static String[] SUPPORTED_STATE = {"RUNNING", "STOPPED"};

    @Override
    protected String[] getSupportedState() {
        return SUPPORTED_STATE;
    }

    /**
     * Run status for this ReportingTask.
     * @return The run status
     */
    @ApiModelProperty(
            value = "The run status of the ReportingTask.",
            allowableValues = "RUNNING, STOPPED"
    )
    public String getState() {
        return super.getState();
    }

}
