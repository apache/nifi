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
package org.apache.nifi.extension.manifest;

import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.Valid;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElementWrapper;
import java.util.List;

@XmlAccessorType(XmlAccessType.FIELD)
public class Restricted {

    private String generalRestrictionExplanation;

    @Valid
    @XmlElementWrapper
    @XmlElement(name = "restriction")
    private List<Restriction> restrictions;

    @Schema(description = "The general restriction for the extension, or null if only specific restrictions exist")
    public String getGeneralRestrictionExplanation() {
        return generalRestrictionExplanation;
    }

    public void setGeneralRestrictionExplanation(String generalRestrictionExplanation) {
        this.generalRestrictionExplanation = generalRestrictionExplanation;
    }

    @Schema(description = "The specific restrictions")
    public List<Restriction> getRestrictions() {
        return restrictions;
    }

    public void setRestrictions(List<Restriction> restrictions) {
        this.restrictions = restrictions;
    }

}
