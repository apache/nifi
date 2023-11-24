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

import org.apache.nifi.util.timebuffer.CountSumMinMaxAccess;
import org.apache.nifi.util.timebuffer.LongEntityAccess;
import org.apache.nifi.util.timebuffer.TimedBuffer;
import org.apache.nifi.util.timebuffer.TimestampedLong;
import org.apache.nifi.util.timebuffer.TimestampedLongAggregation;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Abstract implementation of Leader Election Manager supporting tracking of operations
 */
public abstract class TrackedLeaderElectionManager implements LeaderElectionManager {

    private final ConcurrentMap<String, TimedBuffer<TimestampedLong>> leaderChanges = new ConcurrentHashMap<>();

    private final TimedBuffer<TimestampedLongAggregation> pollTimes = new TimedBuffer<>(TimeUnit.SECONDS, 300, new CountSumMinMaxAccess());

    /**
     * Register as observer without Participant Identifier
     *
     * @param roleName Name of role to be registered for elections
     * @param listener Listener notified on leader state changes
     */
    @Override
    public void register(final String roleName, final LeaderElectionStateChangeListener listener) {
        register(roleName, listener, null);
    }

    @Override
    public Map<String, Integer> getLeadershipChangeCount(final long duration, final TimeUnit unit) {
        final Map<String, Integer> leadershipChangesPerRole = new LinkedHashMap<>();

        for (final Map.Entry<String, TimedBuffer<TimestampedLong>> entry : leaderChanges.entrySet()) {
            final String roleName = entry.getKey();
            final TimedBuffer<TimestampedLong> buffer = entry.getValue();

            final TimestampedLong aggregateValue = buffer.getAggregateValue(System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(duration, unit));
            final int leadershipChanges = (aggregateValue == null) ? 0 : aggregateValue.getValue().intValue();
            leadershipChangesPerRole.put(roleName, leadershipChanges);
        }

        return leadershipChangesPerRole;
    }

    @Override
    public long getAveragePollTime(final TimeUnit timeUnit) {
        final long averageNanos;
        synchronized (pollTimes) {
            final TimestampedLongAggregation.TimestampedAggregation aggregation = pollTimes.getAggregateValue(0L).getAggregation();
            if (aggregation == null || aggregation.getCount() == 0) {
                return 0L;
            }
            averageNanos = aggregation.getSum() / aggregation.getCount();
        }
        return timeUnit.convert(averageNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public long getMinPollTime(final TimeUnit timeUnit) {
        final long minNanos;
        synchronized (pollTimes) {
            final TimestampedLongAggregation.TimestampedAggregation aggregation = pollTimes.getAggregateValue(0L).getAggregation();
            if (aggregation == null) {
                return 0L;
            }
            minNanos = aggregation.getMin();
        }
        return timeUnit.convert(minNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public long getMaxPollTime(final TimeUnit timeUnit) {
        final long maxNanos;
        synchronized (pollTimes) {
            final TimestampedLongAggregation.TimestampedAggregation aggregation = pollTimes.getAggregateValue(0L).getAggregation();
            if (aggregation == null) {
                return 0L;
            }
            maxNanos = aggregation.getMax();
        }
        return timeUnit.convert(maxNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public long getPollCount() {
        synchronized (pollTimes) {
            final TimestampedLongAggregation.TimestampedAggregation aggregation = pollTimes.getAggregateValue(0L).getAggregation();
            if (aggregation == null) {
                return 0L;
            }
            return aggregation.getCount();
        }
    }

    /**
     * Register Poll Time in nanoseconds
     *
     * @param nanos Elapsed System Time in nanoseconds
     */
    protected void registerPollTime(final long nanos) {
        synchronized (pollTimes) {
            pollTimes.add(TimestampedLongAggregation.newValue(nanos));
        }
    }

    /**
     * On Leader Changed register role name changes
     *
     * @param roleName Role Name for leader changes
     */
    protected void onLeaderChanged(final String roleName) {
        final TimedBuffer<TimestampedLong> buffer = leaderChanges.computeIfAbsent(roleName, key -> new TimedBuffer<>(TimeUnit.HOURS, 24, new LongEntityAccess()));
        buffer.add(new TimestampedLong(1L));
    }

    /**
     * Is specified identifier participating in the election based on null or empty participant identifier
     *
     * @param participantId Participant Identifier
     * @return Participating status
     */
    protected boolean isParticipating(final String participantId) {
        return participantId != null && !participantId.trim().isEmpty();
    }
}
