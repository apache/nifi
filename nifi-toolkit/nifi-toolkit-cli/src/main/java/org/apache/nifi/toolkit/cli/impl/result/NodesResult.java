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
import org.apache.nifi.toolkit.cli.impl.result.writer.DynamicTableWriter;
import org.apache.nifi.toolkit.cli.impl.result.writer.Table;
import org.apache.nifi.toolkit.cli.impl.result.writer.TableWriter;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.entity.ClusterEntity;
import org.glassfish.jersey.internal.guava.Lists;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

public class NodesResult extends AbstractWritableResult<ClusterEntity> {

    private final ClusterEntity clusterEntity;

    public NodesResult(ResultType resultType, ClusterEntity clusterEntity) {
        super(resultType);
        this.clusterEntity = clusterEntity;
        Validate.notNull(clusterEntity);
    }

    @Override
    public ClusterEntity getResult() {
        return clusterEntity;
    }

    @Override
    protected void writeSimpleResult(PrintStream output) throws IOException {
        final Table table = new Table.Builder()
                .column("#", 3, 3, false)
                .column("Node ID", 36, 36, false)
                .column("Node Address", 36, 36, true)
                .column("API Port", 8, 8, false)
                .column("Node Status", 13, 13, false)
                .build();

        List<NodeDTO> nodes = Lists.newArrayList(clusterEntity.getCluster().getNodes());
        for (int i = 0; i < nodes.size(); ++i) {
            NodeDTO nodeDTO = nodes.get(i);
            table.addRow(String.valueOf(i), nodeDTO.getNodeId(), nodeDTO.getAddress(), String.valueOf(nodeDTO.getApiPort()), nodeDTO.getStatus());
        }

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }
}
