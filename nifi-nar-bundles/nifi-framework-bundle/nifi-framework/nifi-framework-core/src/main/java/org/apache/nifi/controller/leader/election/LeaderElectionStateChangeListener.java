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
 * Callback interface that can be used to listen for state changes so that the node
 * can be notified when it becomes the Elected Leader for a role or is no longer the
 * Elected Leader
 */
public interface LeaderElectionStateChangeListener {
    /**
     * This method is invoked whenever this node is elected leader
     */
    void onLeaderElection();

    /**
     * This method is invoked whenever this node no longer is the elected leader.
     */
    void onLeaderRelinquish();
}
