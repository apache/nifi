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
package org.apache.nifi.cluster.manager;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;

import java.util.Map;
import java.util.Set;

public class ReportingTasksEntityMerger {

    private static final ReportingTaskEntityMerger reportingTaskEntityMerger = new ReportingTaskEntityMerger();

    /**
     * Merges multiple ReportingTaskEntity responses.
     *
     * @param reportingTaskEntities entities being returned to the client
     * @param entityMap all node responses
     */
    public static void mergeReportingTasks(final Set<ReportingTaskEntity> reportingTaskEntities, final Map<String, Map<NodeIdentifier, ReportingTaskEntity>> entityMap) {
        for (final ReportingTaskEntity entity : reportingTaskEntities) {
            reportingTaskEntityMerger.merge(entity, entityMap.get(entity.getId()));
        }
    }
}
