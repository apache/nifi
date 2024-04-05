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
package org.apache.nifi.questdb.embedded;

enum EmbeddedDatabaseManagerStatus {
    /**
     * Starting status. In this state the manager is not ready to be used. Method {@code QuestDbManager#init} should lead the manager out from this state.
     */
    UNINITIALIZED,

    /**
     * The manager (and the enveloped database) is considered healthy and ready to be used.
     */
    HEALTHY,

    /**
     * The database is in an unexpected state but the manager tries to resolve it. Might end up as {@code EmbeddedQuestDbManagerStatus.HEALTHY}.
     */
    REPAIRING,

    /**
     * The database is in an unexpected state and the manager is not capable to resolve it. This is considered as a "final state".
     */
    CORRUPTED,

    /**
     * The manager is considered shut down and the database is not eligible to work with. This is considered as a "final state"
     */
    CLOSED
}
