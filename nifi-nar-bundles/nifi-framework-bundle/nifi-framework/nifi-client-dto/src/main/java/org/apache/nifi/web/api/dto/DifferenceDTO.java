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

import java.util.Objects;

@XmlType(name = "difference")
public class DifferenceDTO {
    private String differenceType;
    private String difference;

    @ApiModelProperty("The type of difference")
    public String getDifferenceType() {
        return differenceType;
    }

    public void setDifferenceType(String differenceType) {
        this.differenceType = differenceType;
    }

    @ApiModelProperty("Description of the difference")
    public String getDifference() {
        return difference;
    }

    public void setDifference(String difference) {
        this.difference = difference;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final DifferenceDTO that = (DifferenceDTO) o;
        return Objects.equals(differenceType, that.differenceType) && Objects.equals(difference, that.difference);
    }

    @Override
    public int hashCode() {
        return Objects.hash(differenceType, difference);
    }
}
