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

package org.apache.nifi.components.connector.facades.standalone;

import org.apache.nifi.components.connector.components.StatelessGroupLifecycle;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.StatelessGroupNode;

import java.util.concurrent.CompletableFuture;

public class StandaloneStatelessGroupLifecycle implements StatelessGroupLifecycle {
    private final StatelessGroupNode statelessGroupNode;
    private final ProcessScheduler processScheduler;

    public StandaloneStatelessGroupLifecycle(final ProcessGroup processGroup, final ProcessScheduler processScheduler) {
        this.statelessGroupNode = processGroup.getStatelessGroupNode().orElseThrow(() -> new IllegalStateException("Process Group is not configured to run using the Stateless Execution Engine"));
        this.processScheduler = processScheduler;
    }

    @Override
    public CompletableFuture<Void> start() {
        statelessGroupNode.setDesiredState(ScheduledState.RUNNING);
        return processScheduler.startStatelessGroup(statelessGroupNode);
    }

    @Override
    public CompletableFuture<Void> stop() {
        statelessGroupNode.setDesiredState(ScheduledState.STOPPED);
        return processScheduler.stopStatelessGroup(statelessGroupNode);
    }

    // TODO: Stateless Group does not currently support termination.
    @Override
    public CompletableFuture<Void> terminate() {
        return CompletableFuture.completedFuture(null);
    }
}
