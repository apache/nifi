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
package org.apache.nifi.diagnostics.bootstrap.tasks;

import org.apache.nifi.diagnostics.DiagnosticsDumpElement;
import org.apache.nifi.diagnostics.DiagnosticTask;
import org.apache.nifi.diagnostics.StandardDiagnosticsDumpElement;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

public class GarbageCollectionDiagnosticTask implements DiagnosticTask {
    @Override
    public DiagnosticsDumpElement captureDump(final boolean verbose) {
        final List<GarbageCollectorMXBean> garbageCollectors = ManagementFactory.getGarbageCollectorMXBeans();

        final List<String> details = new ArrayList<>();
        for (final GarbageCollectorMXBean garbageCollector : garbageCollectors) {
            details.add(garbageCollector.getName() + " Collection Count : " + garbageCollector.getCollectionCount());
            details.add(garbageCollector.getName() + " Collection Time (ms) : " + garbageCollector.getCollectionTime());
        }

        return new StandardDiagnosticsDumpElement("Garbage Collection", details);
    }
}
