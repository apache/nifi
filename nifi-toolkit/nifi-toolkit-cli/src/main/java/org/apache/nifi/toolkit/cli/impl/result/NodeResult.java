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
package org.apache.nifi.toolkit.cli.impl.result;

import org.apache.commons.lang3.Validate;
import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.entity.NodeEntity;

import java.io.IOException;
import java.io.PrintStream;

public class NodeResult extends AbstractWritableResult<NodeEntity> {

    private final NodeEntity nodeEntity;

    public NodeResult(ResultType resultType, NodeEntity nodeEntity) {
        super(resultType);
        this.nodeEntity = nodeEntity;
        Validate.notNull(nodeEntity);
    }

    @Override
    public NodeEntity getResult() {
        return nodeEntity;
    }

    @Override
    protected void writeSimpleResult(PrintStream output) throws IOException {
        NodeDTO nodeDTO = nodeEntity.getNode();
        output.printf("Node ID: %s\nNode Address: %s\nAPI Port: %s\nNode Status:%s",
                nodeDTO.getNodeId(), nodeDTO.getAddress(), nodeDTO.getApiPort(), nodeDTO.getStatus());
    }
}
