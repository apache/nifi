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
package org.apache.nifi.kubernetes.leader.election.command;

import java.io.Closeable;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Provider abstraction for Kubernetes Leader Election Commands with callbacks
 */
public interface LeaderElectionCommandProvider extends Closeable {
    /**
     * Get Command with required properties
     *
     * @param name Election Name
     * @param identity Election Participant Identity
     * @param onStartLeading Callback run when elected as leader
     * @param onStopLeading Callback run when no longer elected as leader
     * @param onNewLeader Callback run with identification of new leader
     * @return Runnable Command
     */
    Runnable getCommand(
            String name,
            String identity,
            Runnable onStartLeading,
            Runnable onStopLeading,
            Consumer<String> onNewLeader
    );

    /**
     * Find Leader Identifier for specified Election Name
     *
     * @param name Election Name
     * @return Leader Identifier or empty when not found
     */
    Optional<String> findLeader(String name);
}
