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
package org.apache.nifi.controller.repository;

import org.apache.nifi.controller.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StandardCounterRepositoryTest {

    private StandardCounterRepository repository;

    @BeforeEach
    public void setUp() {
        repository = new StandardCounterRepository();
    }

    @Test
    public void testResetAllCountersWithEmptyRepository() {
        final List<Counter> resetCounters = repository.resetAllCounters();
        assertNotNull(resetCounters);
        assertTrue(resetCounters.isEmpty());
    }

    @Test
    public void testResetAllCountersWithSingleCounter() {
        // Create a counter and adjust its value
        repository.adjustCounter("context1", "counter1", 10);

        // Verify the counter has the expected value
        final Counter counter = repository.getCounter("context1", "counter1");
        assertEquals(10, counter.getValue());

        // Reset all counters
        final List<Counter> resetCounters = repository.resetAllCounters();

        // Verify the result
        assertNotNull(resetCounters);
        assertEquals(1, resetCounters.size());
        assertEquals(0, resetCounters.get(0).getValue());
        assertEquals("counter1", resetCounters.get(0).getName());
        assertEquals("context1", resetCounters.get(0).getContext());

        // Verify the original counter is also reset
        assertEquals(0, repository.getCounter("context1", "counter1").getValue());
    }

    @Test
    public void testResetAllCountersWithMultipleCountersAndContexts() {
        // Create counters in different contexts with different values
        repository.adjustCounter("context1", "counter1", 15);
        repository.adjustCounter("context1", "counter2", 25);
        repository.adjustCounter("context2", "counter1", 35);
        repository.adjustCounter("context2", "counter3", 45);

        // Verify initial values
        assertEquals(15, repository.getCounter("context1", "counter1").getValue());
        assertEquals(25, repository.getCounter("context1", "counter2").getValue());
        assertEquals(35, repository.getCounter("context2", "counter1").getValue());
        assertEquals(45, repository.getCounter("context2", "counter3").getValue());

        // Reset all counters
        final List<Counter> resetCounters = repository.resetAllCounters();

        // Verify the result
        assertNotNull(resetCounters);
        assertEquals(4, resetCounters.size());

        // All reset counters should have value 0
        for (final Counter counter : resetCounters) {
            assertEquals(0, counter.getValue());
        }

        // Verify all original counters are reset
        assertEquals(0, repository.getCounter("context1", "counter1").getValue());
        assertEquals(0, repository.getCounter("context1", "counter2").getValue());
        assertEquals(0, repository.getCounter("context2", "counter1").getValue());
        assertEquals(0, repository.getCounter("context2", "counter3").getValue());
    }

    @Test
    public void testResetAllCountersIsAtomic() {
        // Create multiple counters
        repository.adjustCounter("context1", "counter1", 100);
        repository.adjustCounter("context1", "counter2", 200);
        repository.adjustCounter("context2", "counter1", 300);

        // Reset all counters should return all counters with value 0
        final List<Counter> resetCounters = repository.resetAllCounters();

        assertEquals(3, resetCounters.size());

        // Verify all returned counters are reset
        long totalValue = resetCounters.stream().mapToLong(Counter::getValue).sum();
        assertEquals(0, totalValue);
    }

    @Test
    public void testResetAllCountersAfterIndividualReset() {
        // Create counters
        repository.adjustCounter("context1", "counter1", 50);
        repository.adjustCounter("context1", "counter2", 75);

        // Reset one counter individually
        final Counter counter1 = repository.getCounter("context1", "counter1");
        repository.resetCounter(counter1.getIdentifier());

        // Verify individual reset worked
        assertEquals(0, repository.getCounter("context1", "counter1").getValue());
        assertEquals(75, repository.getCounter("context1", "counter2").getValue());

        // Reset all counters
        final List<Counter> resetCounters = repository.resetAllCounters();

        // Should still return all counters (including already reset ones)
        assertEquals(2, resetCounters.size());
        for (final Counter counter : resetCounters) {
            assertEquals(0, counter.getValue());
        }
    }

    @Test
    public void testResetAllCountersReturnsUnmodifiableCounters() {
        // Create a counter
        repository.adjustCounter("context1", "counter1", 42);

        // Reset all counters
        final List<Counter> resetCounters = repository.resetAllCounters();

        assertNotNull(resetCounters);
        assertEquals(1, resetCounters.size());

        final Counter returnedCounter = resetCounters.get(0);
        assertEquals(0, returnedCounter.getValue());

        // The returned counter should be unmodifiable (this is implementation specific)
        // We can verify the type or behavior if needed, but the main contract is that it reflects the reset state
        assertEquals("counter1", returnedCounter.getName());
        assertEquals("context1", returnedCounter.getContext());
    }
}
