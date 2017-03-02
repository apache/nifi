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
import java.util.Collection;
import java.util.Date;

import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.nifi.web.api.dto.util.TimeAdapter;

/**
 * Details about the composition of the cluster at a specific date/time.
 */
@XmlType(name = "cluster")
public class ClusterDTO {

    private Collection<NodeDTO> nodes;
    private Date generated;

    /**
     * @return collection of the node DTOs
     */
    @ApiModelProperty(
            value = "The collection of nodes that are part of the cluster."
    )
    public Collection<NodeDTO> getNodes() {
        return nodes;
    }

    public void setNodes(Collection<NodeDTO> nodes) {
        this.nodes = nodes;
    }

    /**
     * @return the date/time that this report was generated
     */
    @XmlJavaTypeAdapter(TimeAdapter.class)
    @ApiModelProperty(
            value = "The timestamp the report was generated.",
            dataType = "string"
    )
    public Date getGenerated() {
        return generated;
    }

    public void setGenerated(Date generated) {
        this.generated = generated;
    }
}
