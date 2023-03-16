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
package org.apache.nifi.processors.grpc.util;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class BackpressureChecker {

    public static final int RECHECK_THRESHOLD = 5;

    private final ProcessContext processContext;
    private final Set<Relationship> relationships;
    private final AtomicLong requestCount = new AtomicLong(0L);
    private final AtomicBoolean backPressure = new AtomicBoolean(false);

    public BackpressureChecker(ProcessContext processContext, Set<Relationship> relationships) {
        this.processContext = processContext;
        this.relationships = relationships;
    }

    public boolean isBackpressure() {
        long n = requestCount.getAndIncrement() % RECHECK_THRESHOLD;
        if (n == 0 || backPressure.get()) {
            backPressure.set(processContext.getAvailableRelationships().size() != relationships.size());
        }
        return backPressure.get();
    }
}
