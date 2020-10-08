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

package org.apache.nifi.controller;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestStandardFunnel {

    @Test
    public void testSetFlowFileLimit() {
        {
            StandardFunnel funnel = getStandardFunnel(1, 100000);
            assertEquals(1, funnel.getMaxConcurrentTasks());
            assertEquals(100, funnel.maxIterations);
        }
        {
            StandardFunnel funnel = getStandardFunnel(1, 100001);
            assertEquals(1, funnel.getMaxConcurrentTasks());
            assertEquals(101, funnel.maxIterations);
        }
        {
            StandardFunnel funnel = getStandardFunnel(1, 99999);
            assertEquals(1, funnel.getMaxConcurrentTasks());
            assertEquals(100, funnel.maxIterations);
        }
        {
            StandardFunnel funnel = getStandardFunnel(1, 0);
            assertEquals(1, funnel.getMaxConcurrentTasks());
            assertEquals(1, funnel.maxIterations);
        }
        {
            StandardFunnel funnel = getStandardFunnel(1, 1);
            assertEquals(1, funnel.getMaxConcurrentTasks());
            assertEquals(1, funnel.maxIterations);
        }
    }

    private StandardFunnel getStandardFunnel(final int maxConcurrentTasks, final int maxBatchSize) {
        return new StandardFunnel("1", maxConcurrentTasks, maxBatchSize);
    }
}