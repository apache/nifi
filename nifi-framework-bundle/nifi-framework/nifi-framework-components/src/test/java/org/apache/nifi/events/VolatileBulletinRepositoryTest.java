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
package org.apache.nifi.events;

import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinQuery;
import org.apache.nifi.reporting.ComponentType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class VolatileBulletinRepositoryTest {

    private VolatileBulletinRepository repository;

    @BeforeEach
    public void setUp() {
        repository = new VolatileBulletinRepository();
    }

    @Test
    public void testClearBulletinsForSingleComponent() {
        // Setup - add some bulletins
        final String componentId = "test-component-1";
        final Instant baseTime = Instant.now();
        final Instant clearTime = baseTime.plusSeconds(5); // 5 seconds later

        // Add bulletins before, at, and after clear time
        addBulletin(componentId, "Before clear", Date.from(baseTime.plusSeconds(1)));
        addBulletin(componentId, "At clear time", Date.from(clearTime));
        addBulletin(componentId, "After clear", Date.from(baseTime.plusSeconds(10)));
        addBulletin("other-component", "Other component", Date.from(baseTime.plusSeconds(6)));

        // Initial count should be 4
        assertEquals(4, getBulletinCount());

        // Clear bulletins older than or equal to clearTime
        int cleared = repository.clearBulletinsForComponent(componentId, clearTime);

        // Should have cleared 2 bulletins (before clear time + at clear time)
        assertEquals(2, cleared);

        // Should have 2 bulletins remaining (after clear + other component)
        assertEquals(2, getBulletinCount());
    }

    @Test
    public void testClearBulletinsForMultipleComponents() {
        // Setup - add bulletins for multiple components
        final Instant baseTime = Instant.now();
        final Instant clearTime = baseTime.plusSeconds(5);

        addBulletin("component-1", "Component 1 before", Date.from(baseTime.plusSeconds(1)));
        addBulletin("component-1", "Component 1 after", Date.from(baseTime.plusSeconds(6)));
        addBulletin("component-2", "Component 2 before", Date.from(baseTime.plusSeconds(2)));
        addBulletin("component-2", "Component 2 after", Date.from(baseTime.plusSeconds(7)));
        addBulletin("component-3", "Component 3 after", Date.from(baseTime.plusSeconds(8)));

        assertEquals(5, getBulletinCount());

        // Clear bulletins for components 1 and 2 older than clearTime
        int cleared = repository.clearBulletinsForComponents(
                Arrays.asList("component-1", "component-2"), clearTime);

        // Should have cleared 2 bulletins (component-1 before + component-2 before)
        assertEquals(2, cleared);

        // Should have 3 bulletins remaining (component-1 after + component-2 after + component-3 after)
        assertEquals(3, getBulletinCount());
    }

    @Test
    public void testClearBulletinsWithNullSourceId() {
        assertThrows(IllegalArgumentException.class, () -> {
            repository.clearBulletinsForComponent(null, Instant.now());
        });
    }

    @Test
    public void testClearBulletinsWithNullTimestamp() {
        assertThrows(IllegalArgumentException.class, () -> {
            repository.clearBulletinsForComponent("test-component", null);
        });
    }

    @Test
    public void testClearBulletinsForComponentsWithNullCollection() {
        // Should throw exception when null collection is provided
        assertThrows(IllegalArgumentException.class, () -> {
            repository.clearBulletinsForComponents(null, Instant.now());
        });
    }

    @Test
    public void testClearBulletinsForComponentsWithEmptyCollection() {
        // Should throw exception when empty collection is provided
        assertThrows(IllegalArgumentException.class, () -> {
            repository.clearBulletinsForComponents(Collections.emptyList(), Instant.now());
        });
    }

    @Test
    public void testClearBulletinsForComponentsWithNullTimestamp() {
        assertThrows(IllegalArgumentException.class, () -> {
            repository.clearBulletinsForComponents(Arrays.asList("component-1"), null);
        });
    }

    @Test
    public void testClearBulletinsExactTimestampMatch() {
        final String componentId = "test-component";
        final Instant exactTime = Instant.now();

        // Add bulletin exactly at the clear time
        addBulletin(componentId, "Exact time", Date.from(exactTime));
        addBulletin(componentId, "Before", Date.from(exactTime.minusSeconds(1)));
        addBulletin(componentId, "After", Date.from(exactTime.plusSeconds(1)));

        assertEquals(3, getBulletinCount());

        // Clear bulletins older than or equal to exact time
        int cleared = repository.clearBulletinsForComponent(componentId, exactTime);

        // Should clear bulletins at and before exact time
        assertEquals(2, cleared);
        assertEquals(1, getBulletinCount());
    }

    @Test
    public void testClearBulletinsNonExistentComponent() {
        // Add some bulletins
        addBulletin("existing-component", "Test message", new Date());
        assertEquals(1, getBulletinCount());

        // Try to clear bulletins for non-existent component
        int cleared = repository.clearBulletinsForComponent("non-existent", Instant.now());

        // Should clear 0 bulletins
        assertEquals(0, cleared);
        assertEquals(1, getBulletinCount());
    }

    @Test
    public void testClearBulletinsSingleComponentDelegatesToMultiple() {
        // Verify that single component method delegates to multiple components method
        final String componentId = "test-component";
        final Instant clearTime = Instant.now();

        addBulletin(componentId, "Test message", Date.from(clearTime.plusSeconds(1)));

        // Clear using single component method
        int clearedSingle = repository.clearBulletinsForComponent(componentId, clearTime);

        // Add same bulletin again
        addBulletin(componentId, "Test message", Date.from(clearTime.plusSeconds(1)));

        // Clear using multiple components method with single component
        int clearedMultiple = repository.clearBulletinsForComponents(
                Collections.singletonList(componentId), clearTime);

        // Results should be the same
        assertEquals(clearedSingle, clearedMultiple);
    }

    private void addBulletin(String sourceId, String message, Date timestamp) {
        TestBulletin bulletin = new TestBulletin(sourceId, message, timestamp);
        repository.addBulletin(bulletin);
    }

    private static class TestBulletin extends Bulletin {
        private final String sourceId;
        private final String message;
        private final Date timestamp;

        public TestBulletin(String sourceId, String message, Date timestamp) {
            super(System.nanoTime());
            this.sourceId = sourceId;
            this.message = message;
            this.timestamp = timestamp;
        }

        @Override
        public String getSourceId() {
            return sourceId;
        }

        @Override
        public String getMessage() {
            return message;
        }

        @Override
        public Date getTimestamp() {
            return timestamp;
        }

        @Override
        public String getCategory() {
            return "Test";
        }

        @Override
        public String getLevel() {
            return "INFO";
        }

        @Override
        public ComponentType getSourceType() {
            return ComponentType.PROCESSOR;
        }

        @Override
        public String getSourceName() {
            return "Test Source";
        }

        @Override
        public String getGroupId() {
            return "test-group";
        }

        @Override
        public String getGroupName() {
            return "Test Group";
        }

        @Override
        public String getGroupPath() {
            return "/";
        }

        @Override
        public String getNodeAddress() {
            return "localhost";
        }

        @Override
        public String getFlowFileUuid() {
            return null;
        }
    }

    private int getBulletinCount() {
        BulletinQuery query = new BulletinQuery.Builder().build();
        List<Bulletin> bulletins = repository.findBulletins(query);
        return bulletins.size();
    }
}
