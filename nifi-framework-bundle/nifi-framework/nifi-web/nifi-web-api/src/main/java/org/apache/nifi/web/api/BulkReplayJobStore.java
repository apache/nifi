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
package org.apache.nifi.web.api;

import java.util.Collection;
import java.util.Optional;

/**
 * Stores bulk replay job state on the primary node.
 *
 * <p>All job state lives exclusively on the primary node. Non-primary nodes forward
 * requests to the primary via cluster replication. All state is in-memory and lost
 * on restart.</p>
 */
public interface BulkReplayJobStore {

    /**
     * Registers a new job.
     */
    void registerJob(BulkReplayJob job);

    /**
     * Returns the job with the given id, or empty if not found.
     */
    Optional<BulkReplayJob> findJobById(String jobId);

    /**
     * Returns all jobs known to this node.
     */
    Collection<BulkReplayJob> getAllJobs();

    /**
     * Removes all state for the given job id.
     */
    void remove(String jobId);
}
