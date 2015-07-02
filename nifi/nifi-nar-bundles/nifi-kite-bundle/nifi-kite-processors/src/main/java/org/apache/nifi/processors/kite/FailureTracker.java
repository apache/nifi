/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.nifi.processors.kite;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import java.util.Map;

class FailureTracker {
    private static final Splitter REASON_SEPARATOR = Splitter.on(':').limit(2);

    private final Map<String, String> examples = Maps.newLinkedHashMap();
    private final Map<String, Integer> occurrences = Maps.newLinkedHashMap();
    long count = 0L;

    public void add(Throwable throwable) {
        add(reason(throwable));
    }

    public void add(String reason) {
        count += 1;
        String problem = Iterators.getNext(REASON_SEPARATOR.split(reason).iterator(), "Unknown");
        if (examples.containsKey(problem)) {
            occurrences.put(problem, occurrences.get(problem) + 1);
        } else {
            examples.put(problem, reason);
            occurrences.put(problem, 1);
        }
    }

    public long count() {
        return count;
    }

    public String summary() {
        boolean first = true;
        StringBuilder sb = new StringBuilder();
        for (String problem : examples.keySet()) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append(examples.get(problem));
            int similar = occurrences.get(problem) - 1;
            if (similar == 1) {
                sb.append(" (1 similar failure)");
            } else if (similar > 1) {
                sb.append(" (").append(similar).append(" similar failures)");
            }
        }
        return sb.toString();
    }

    private static String reason(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable current = t; current != null; current = current.getCause()) {
            if (current != t) {
                sb.append(": ");
            }
            sb.append(current.getMessage());
        }
        return sb.toString();
    }
}
