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

package org.apache.nifi.registry.diff;

import io.swagger.annotations.ApiModelProperty;

/**
 * Represents a specific, individual difference that has changed between 2 versions.
 * The change data and textual descriptions of the change are included for client consumption.
 */
public class ComponentDifference {
    private String valueA;
    private String valueB;
    private String changeDescription;
    private String differenceType;
    private String differenceTypeDescription;

    @ApiModelProperty("The earlier value from the difference.")
    public String getValueA() {
        return valueA;
    }

    public void setValueA(String valueA) {
        this.valueA = valueA;
    }

    @ApiModelProperty("The newer value from the difference.")
    public String getValueB() {
        return valueB;
    }

    public void setValueB(String valueB) {
        this.valueB = valueB;
    }

    @ApiModelProperty("The description of the change.")
    public String getChangeDescription() {
        return changeDescription;
    }

    public void setChangeDescription(String changeDescription) {
        this.changeDescription = changeDescription;
    }

    @ApiModelProperty("The key to the difference.")
    public String getDifferenceType() {
        return differenceType;
    }

    public void setDifferenceType(String differenceType) {
        this.differenceType = differenceType;
    }

    @ApiModelProperty("The description of the change type.")
    public String getDifferenceTypeDescription() {
        return differenceTypeDescription;
    }

    public void setDifferenceTypeDescription(String differenceTypeDescription) {
        this.differenceTypeDescription = differenceTypeDescription;
    }
}
