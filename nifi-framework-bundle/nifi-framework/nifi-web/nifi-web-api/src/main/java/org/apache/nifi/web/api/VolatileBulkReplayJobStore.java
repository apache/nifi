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

import org.apache.nifi.web.api.dto.bulkreplay.BulkReplayJobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory (volatile) {@link BulkReplayJobStore}.
 *
 * <p>All job state lives on the primary node only. All state is lost on restart.</p>
 *
 * <p>When the number of jobs exceeds {@code maxJobs}, the oldest terminal (completed,
 * failed, cancelled, partial-success) job is evicted automatically. Active jobs (QUEUED, RUNNING,
 * INTERRUPTED) are never evicted because INTERRUPTED jobs are automatically resumed when a new
 * primary node is elected.</p>
 */
public class VolatileBulkReplayJobStore implements BulkReplayJobStore {

    private static final Logger logger = LoggerFactory.getLogger(VolatileBulkReplayJobStore.class);

    private static final Set<BulkReplayJobStatus> ACTIVE_STATUSES = Set.of(
            BulkReplayJobStatus.QUEUED,
            BulkReplayJobStatus.RUNNING,
            BulkReplayJobStatus.INTERRUPTED
    );

    private final Map<String, BulkReplayJob> jobs = new ConcurrentHashMap<>();
    private final int maxJobs;

    public VolatileBulkReplayJobStore(final int maxJobs) {
        this.maxJobs = maxJobs;
    }

    @Override
    public void registerJob(final BulkReplayJob job) {
        jobs.put(job.getJobId(), job);
        evictIfNeeded();
    }

    @Override
    public Optional<BulkReplayJob> findJobById(final String jobId) {
        return Optional.ofNullable(jobs.get(jobId));
    }

    @Override
    public Collection<BulkReplayJob> getAllJobs() {
        return Collections.unmodifiableCollection(jobs.values());
    }

    @Override
    public void remove(final String jobId) {
        jobs.remove(jobId);
    }

    /**
     * Evicts the oldest terminal job when the store exceeds {@code maxJobs}.
     * Uses {@code submissionTime} for ordering. Active jobs are never evicted.
     */
    private void evictIfNeeded() {
        while (jobs.size() > maxJobs) {
            final Optional<BulkReplayJob> oldest = jobs.values().stream()
                    .filter(j -> j.getStatus() != null && !ACTIVE_STATUSES.contains(j.getStatus()))
                    .min(Comparator.comparing(
                            BulkReplayJob::getSubmissionTime,
                            Comparator.nullsLast(Comparator.naturalOrder())
                    ));

            if (oldest.isPresent()) {
                final String evictedId = oldest.get().getJobId();
                logger.debug("Evicting bulk replay job {} (max jobs {} exceeded)", evictedId, maxJobs);
                remove(evictedId);
            } else {
                break;
            }
        }
    }
}
