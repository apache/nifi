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
package org.apache.nifi.web.api.dto.provenance;

import io.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlType;

/**
 * Provenance value used to query
 */
@XmlType(name = "provenanceSearchValue")
public class ProvenanceSearchValueDTO {

    private String value;
    private Boolean inverse;

    /**
     * @return the search value
     */
    @ApiModelProperty(
            value = "The search value."
    )
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    /**
     * @return whether inverse of search value should be searched or not
     */
    @ApiModelProperty(
            value = "Query for all except for search value."
    )
    public Boolean getInverse() {
        return inverse;
    }

    public void setInverse(Boolean inverse) {
        this.inverse = inverse;
    }
}