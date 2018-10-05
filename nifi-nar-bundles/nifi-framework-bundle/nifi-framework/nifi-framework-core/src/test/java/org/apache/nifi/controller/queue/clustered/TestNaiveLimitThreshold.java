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

package org.apache.nifi.controller.queue.clustered;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestNaiveLimitThreshold {

    @Test
    public void testCount() {
        final SimpleLimitThreshold threshold = new SimpleLimitThreshold(10, 100L);
        for (int i = 0; i < 9; i++) {
            threshold.adjust(1, 1L);
            assertFalse(threshold.isThresholdMet());
        }

        threshold.adjust(1, 1L);
        assertTrue(threshold.isThresholdMet());
    }

    @Test
    public void testSize() {
        final SimpleLimitThreshold threshold = new SimpleLimitThreshold(10, 100L);
        for (int i = 0; i < 9; i++) {
            threshold.adjust(0, 10L);
            assertFalse(threshold.isThresholdMet());
        }

        threshold.adjust(1, 9L);
        assertFalse(threshold.isThresholdMet());

        threshold.adjust(-1, 1L);
        assertTrue(threshold.isThresholdMet());

        threshold.adjust(0, -1L);
        assertFalse(threshold.isThresholdMet());

        threshold.adjust(-10, 10000L);
        assertTrue(threshold.isThresholdMet());
    }

}
