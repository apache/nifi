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
package org.apache.nifi.controller.status.analytics;

import java.util.List;
import java.util.Map.Entry;

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.status.history.ComponentStatusRepository;
import org.apache.nifi.controller.status.history.StatusHistoryUtil;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO;
import org.apache.nifi.web.api.dto.status.StatusSnapshotDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatusAnalyticEngine {
    private ComponentStatusRepository statusRepository;
    private FlowController controller;

    private static final Logger LOG = LoggerFactory.getLogger(StatusAnalyticEngine.class);

    public StatusAnalyticEngine(FlowController controller, ComponentStatusRepository statusRepository) {
        this.controller = controller;
        this.statusRepository = statusRepository;
    }

    public long getMinTimeToBackpressure() {
        ProcessGroup rootGroup = controller.getFlowManager().getRootGroup();
        List<Connection> allConnections = rootGroup.findAllConnections();

        for (Connection conn : allConnections) {
            LOG.info("Getting connection history for: " + conn.getIdentifier());
            StatusHistoryDTO connHistory = StatusHistoryUtil.createStatusHistoryDTO(
                    statusRepository.getConnectionStatusHistory(conn.getIdentifier(), null, null, Integer.MAX_VALUE));
            for (StatusSnapshotDTO snap : connHistory.getAggregateSnapshots()) {
                for (Entry<String, Long> snapEntry : snap.getStatusMetrics().entrySet()) {
                    LOG.info("Snap " + snapEntry.getKey() + ": " + snapEntry.getValue());
                }
            }
        }

        return 0;
    }
}
