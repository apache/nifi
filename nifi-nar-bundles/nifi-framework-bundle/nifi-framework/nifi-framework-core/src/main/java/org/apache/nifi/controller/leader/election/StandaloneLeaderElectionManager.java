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

package org.apache.nifi.controller.leader.election;

/**
 * <p>
 * A LeaderElectionManager to use when running a standalone (un-clustered) NiFi instance
 * </p>
 */
public class StandaloneLeaderElectionManager implements LeaderElectionManager {

    @Override
    public void start() {
    }

    @Override
    public void register(final String roleName, final LeaderElectionStateChangeListener listener) {
    }

    @Override
    public void register(final String roleName, final LeaderElectionStateChangeListener listener, final String participantId) {
    }

    @Override
    public String getLeader(final String roleName) {
        return null;
    }

    @Override
    public void unregister(final String roleName) {
    }

    @Override
    public boolean isLeader(final String roleName) {
        return false;
    }

    @Override
    public boolean isStopped() {
        return false;
    }

    @Override
    public void stop() {
    }

    @Override
    public boolean isLeaderElected(String roleName) {
        return false;
    }
}
