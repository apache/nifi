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
package org.apache.nifi.web.api.dto.provenance.lineage;

import com.wordnik.swagger.annotations.ApiModelProperty;
import java.util.List;
import java.util.Set;
import javax.xml.bind.annotation.XmlType;

/**
 * Represents the lineage results.
 */
@XmlType(name = "lineage")
public class LineageResultsDTO {

    private Set<String> errors;

    private List<ProvenanceNodeDTO> nodes;
    private List<ProvenanceLinkDTO> links;

    /**
     * @return any error messages
     */
    @ApiModelProperty(
            value = "Any errors that occurred while generating the lineage."
    )
    public Set<String> getErrors() {
        return errors;
    }

    public void setErrors(Set<String> errors) {
        this.errors = errors;
    }

    /**
     * @return the nodes
     */
    @ApiModelProperty(
            value = "The nodes in the lineage."
    )
    public List<ProvenanceNodeDTO> getNodes() {
        return nodes;
    }

    public void setNodes(List<ProvenanceNodeDTO> nodes) {
        this.nodes = nodes;
    }

    /**
     * @return the links
     */
    @ApiModelProperty(
            value = "The links between the nodes in the lineage."
    )
    public List<ProvenanceLinkDTO> getLinks() {
        return links;
    }

    public void setLinks(List<ProvenanceLinkDTO> links) {
        this.links = links;
    }

}
