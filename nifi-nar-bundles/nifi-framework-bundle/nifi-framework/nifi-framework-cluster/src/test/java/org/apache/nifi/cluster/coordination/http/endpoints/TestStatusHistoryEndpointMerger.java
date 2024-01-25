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

package org.apache.nifi.cluster.coordination.http.endpoints;

import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestStatusHistoryEndpointMerger {
    @Test
    public void testNormalizedStatusSnapshotDate() {
        final Date date1 = new Date(1388538000000L);
        final Date date2 = new Date(1388538299999L);
        final Date date3 = new Date(1388538300000L);
        final Date date4 = new Date(1388538300001L);

        final Date normalized1 = StatusHistoryEndpointMerger.normalizeStatusSnapshotDate(date1, 300000);
        assertEquals(date1, normalized1);

        final Date normalized2 = StatusHistoryEndpointMerger.normalizeStatusSnapshotDate(date2, 300000);
        assertEquals(date1, normalized2);

        final Date normalized3 = StatusHistoryEndpointMerger.normalizeStatusSnapshotDate(date3, 300000);
        assertEquals(date3, normalized3);

        final Date normalized4 = StatusHistoryEndpointMerger.normalizeStatusSnapshotDate(date4, 300000);
        assertEquals(date3, normalized4);
    }
}
