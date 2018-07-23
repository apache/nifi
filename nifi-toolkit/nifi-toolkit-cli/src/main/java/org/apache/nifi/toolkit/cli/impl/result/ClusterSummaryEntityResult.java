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
import org.apache.nifi.web.api.entity.ClusteSummaryEntity;

import java.io.PrintStream;

/**
 * Result for CurrentUserEntity from NiFi.
 */
public class ClusterSummaryEntityResult extends AbstractWritableResult<ClusteSummaryEntity> {

    private final ClusteSummaryEntity clusteSummaryEntity;

    public ClusterSummaryEntityResult(final ResultType resultType, final ClusteSummaryEntity clusteSummaryEntity) {
        super(resultType);
        this.clusteSummaryEntity = clusteSummaryEntity;
        Validate.notNull(this.clusteSummaryEntity);
    }

    @Override
    public ClusteSummaryEntity getResult() {
        return clusteSummaryEntity;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) {
        output.printf(
            "Total node count: %d\nConnected node count: %d\nClustered: %s\nConnected to cluster: %s\n",
            clusteSummaryEntity.getClusterSummary().getTotalNodeCount(),
            clusteSummaryEntity.getClusterSummary().getConnectedNodeCount(),
            clusteSummaryEntity.getClusterSummary().getClustered(),
            clusteSummaryEntity.getClusterSummary().getConnectedToCluster()
        );
    }

}
