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
package org.apache.nifi.reporting.sql;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.sql.util.TrackedQueryTime;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public interface QueryTimeAware {

    default String processStartAndEndTimes(ReportingContext context, String sql, TrackedQueryTime queryStartTime, TrackedQueryTime queryEndTime) throws IOException {
        StateManager stateManager = context.getStateManager();
        final Map<String, String> stateMap = new HashMap<>(stateManager.getState(Scope.LOCAL).toMap());

        if (sql.contains(queryStartTime.getSqlPlaceholder()) && sql.contains(queryEndTime.getSqlPlaceholder())) {
            final long startTime = stateMap.get(queryStartTime.name()) == null ? 0 : Long.parseLong(stateMap.get(queryStartTime.name()));
            final long currentTime = getCurrentTime();

            sql = sql.replace(queryStartTime.getSqlPlaceholder(), String.valueOf(startTime));
            sql = sql.replace(queryEndTime.getSqlPlaceholder(), String.valueOf(currentTime));

            stateMap.put(queryStartTime.name(), String.valueOf(currentTime));
            stateManager.setState(stateMap, Scope.LOCAL);
        }
        return sql;
    }

    default long getCurrentTime() {
        return Instant.now().toEpochMilli();
    }
}
