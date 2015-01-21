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
package org.apache.nifi.web.api.dto.status;

import javax.xml.bind.annotation.XmlType;
import org.apache.nifi.web.api.dto.NodeDTO;

/**
 * DTO for serializing the processor status for a particular node.
 */
@XmlType(name = "nodeProcessorStatus")
public class NodeProcessorStatusDTO {

    private NodeDTO node;
    private ProcessorStatusDTO processorStatus;

    /**
     * The node.
     *
     * @return
     */
    public NodeDTO getNode() {
        return node;
    }

    public void setNode(NodeDTO node) {
        this.node = node;
    }

    /**
     * The processor's status.
     *
     * @return
     */
    public ProcessorStatusDTO getProcessorStatus() {
        return processorStatus;
    }

    public void setProcessorStatus(ProcessorStatusDTO processorStatus) {
        this.processorStatus = processorStatus;
    }

}
