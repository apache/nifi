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

import io.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlType;

@XmlType(name = "priorityRule")
public class PriorityRuleDTO {
    private String id;
    private String expression;
    private String label;
    private int rateOfThreadUsage;
    private boolean expired;

    @ApiModelProperty(value = "The id of the priority rule")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @ApiModelProperty(value = "The expression of the priority rule")
    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    @ApiModelProperty(value = "An optional human readable label for the priority rule")
    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    @ApiModelProperty(value = "The % of the time files of this priority will be given to an available thread for processing when polled")
    public int getRateOfThreadUsage() {
        return rateOfThreadUsage;
    }

    public void setRateOfThreadUsage(int rateOfThreadUsage) {
        this.rateOfThreadUsage = rateOfThreadUsage;
    }

    @ApiModelProperty(value = "Whether or not the priority rule is expired")
    public boolean isExpired() {
        return expired;
    }

    public void setExpired(boolean expired) {
        this.expired = expired;
    }

    @Override
    public String toString() {
        return "PriorityRuleDTO{" +
                "id='" + id + '\'' +
                ", expression='" + expression + '\'' +
                ", label='" + label + '\'' +
                ", rateOfThreadUsage=" + rateOfThreadUsage +
                ", expired=" + expired +
                '}';
    }
}
