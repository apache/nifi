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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link VolatileBulkReplayJobStore}. */
class TestVolatileBulkReplayJobStore {

    private VolatileBulkReplayJobStore store;

    @BeforeEach
    void setUp() {
        store = new VolatileBulkReplayJobStore(1000);
    }

    @Test
    void testGetAllJobs_emptyByDefault() {
        assertTrue(store.getAllJobs().isEmpty());
    }

    @Test
    void testRegisterJobAndFindJobById() {
        final BulkReplayJob job = job("j1");
        store.registerJob(job);

        assertTrue(store.findJobById("j1").isPresent());
        assertSame(job, store.findJobById("j1").get());
    }

    @Test
    void testGetAllJobs_returnsAllRegisteredJobs() {
        store.registerJob(job("a"));
        store.registerJob(job("b"));
        store.registerJob(job("c"));

        assertEquals(3, store.getAllJobs().size());
    }

    @Test
    void testFindJobById_returnsEmptyWhenMissing() {
        assertTrue(store.findJobById("no-such-id").isEmpty());
    }

    @Test
    void testRemove_existingJobIsGone() {
        store.registerJob(job("x"));

        store.remove("x");

        assertTrue(store.findJobById("x").isEmpty());
        assertTrue(store.getAllJobs().isEmpty());
    }

    @Test
    void testRemove_nonExistentIsNoOp() {
        // Should not throw
        store.remove("ghost");
    }

    @Test
    void testRegisterJob_sameIdReplacesEntry() {
        final BulkReplayJob first = job("dup");
        first.setStatus(BulkReplayJobStatus.RUNNING);
        store.registerJob(first);

        final BulkReplayJob second = job("dup");
        second.setStatus(BulkReplayJobStatus.COMPLETED);
        store.registerJob(second);

        assertEquals(1, store.getAllJobs().size());
        assertSame(second, store.findJobById("dup").get());
    }

    @Test
    void testGetAllJobs_isUnmodifiable() {
        store.registerJob(job("z"));
        final Collection<BulkReplayJob> snapshot = store.getAllJobs();
        assertThrows(UnsupportedOperationException.class, () -> ((Collection<BulkReplayJob>) snapshot).add(job("extra")));
    }

    @Test
    void testEviction_oldestTerminalJobEvictedWhenMaxExceeded() {
        final VolatileBulkReplayJobStore smallStore = new VolatileBulkReplayJobStore(3);

        // Register 3 completed jobs with ascending submission times
        for (int i = 1; i <= 3; i++) {
            final BulkReplayJob j = jobAt("job-" + i, Instant.parse("2024-01-0" + i + "T00:00:00Z"));
            j.setStatus(BulkReplayJobStatus.COMPLETED);
            smallStore.registerJob(j);
        }
        assertEquals(3, smallStore.getAllJobs().size());

        // Adding a 4th should evict the oldest (job-1)
        final BulkReplayJob fourth = jobAt("job-4", Instant.parse("2024-01-04T00:00:00Z"));
        fourth.setStatus(BulkReplayJobStatus.QUEUED);
        smallStore.registerJob(fourth);

        assertEquals(3, smallStore.getAllJobs().size());
        assertTrue(smallStore.findJobById("job-1").isEmpty(), "Oldest completed job should be evicted");
        assertTrue(smallStore.findJobById("job-4").isPresent(), "New job should be present");
    }

    @Test
    void testEviction_activeJobsNeverEvicted() {
        final VolatileBulkReplayJobStore smallStore = new VolatileBulkReplayJobStore(2);

        // Register 2 active (RUNNING) jobs
        final BulkReplayJob running1 = jobAt("r1", Instant.parse("2024-01-01T00:00:00Z"));
        running1.setStatus(BulkReplayJobStatus.RUNNING);
        smallStore.registerJob(running1);

        final BulkReplayJob running2 = jobAt("r2", Instant.parse("2024-01-02T00:00:00Z"));
        running2.setStatus(BulkReplayJobStatus.RUNNING);
        smallStore.registerJob(running2);

        // Adding a 3rd — no terminal jobs to evict, so all 3 stay
        final BulkReplayJob third = jobAt("r3", Instant.parse("2024-01-03T00:00:00Z"));
        third.setStatus(BulkReplayJobStatus.QUEUED);
        smallStore.registerJob(third);

        assertEquals(3, smallStore.getAllJobs().size(), "Active jobs should not be evicted");
    }

    @Test
    void testEviction_interruptedJobsNeverEvicted() {
        final VolatileBulkReplayJobStore smallStore = new VolatileBulkReplayJobStore(2);

        // Register an INTERRUPTED and a COMPLETED job — both fit in maxJobs=2
        final BulkReplayJob interrupted = jobAt("i1", Instant.parse("2024-01-01T00:00:00Z"));
        interrupted.setStatus(BulkReplayJobStatus.INTERRUPTED);
        smallStore.registerJob(interrupted);

        final BulkReplayJob completed = jobAt("i2", Instant.parse("2024-01-02T00:00:00Z"));
        completed.setStatus(BulkReplayJobStatus.COMPLETED);
        smallStore.registerJob(completed);

        assertEquals(2, smallStore.getAllJobs().size());

        // Adding a 3rd — should evict the COMPLETED one, NOT the INTERRUPTED one
        final BulkReplayJob third = jobAt("i3", Instant.parse("2024-01-03T00:00:00Z"));
        third.setStatus(BulkReplayJobStatus.QUEUED);
        smallStore.registerJob(third);

        assertEquals(2, smallStore.getAllJobs().size());
        assertTrue(smallStore.findJobById("i1").isPresent(), "INTERRUPTED jobs should not be evicted");
        assertTrue(smallStore.findJobById("i2").isEmpty(), "COMPLETED job should be evicted");
        assertTrue(smallStore.findJobById("i3").isPresent(), "New job should be present");
    }

    // -----------------------------------------------------------------------

    private static BulkReplayJob job(final String id) {
        return new BulkReplayJob(id, "Test Job", "user", Instant.now(),
                "proc-1", "TestProcessor", "org.apache.nifi.TestProcessor", "group-1", List.of());
    }

    private static BulkReplayJob jobAt(final String id, final Instant submissionTime) {
        return new BulkReplayJob(id, "Test Job", "user", submissionTime,
                "proc-1", "TestProcessor", "org.apache.nifi.TestProcessor", "group-1", List.of());
    }
}
