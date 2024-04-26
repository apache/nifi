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

import org.apache.nifi.diagnostics.DiagnosticTask;
import org.apache.nifi.diagnostics.DiagnosticsDumpElement;
import org.apache.nifi.diagnostics.StandardDiagnosticsDumpElement;
import org.apache.nifi.python.BoundObjectCounts;
import org.apache.nifi.python.PythonBridge;
import org.apache.nifi.python.PythonProcessorDetails;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class PythonBridgeDiagnosticTask implements DiagnosticTask {
    private final PythonBridge pythonBridge;

    public PythonBridgeDiagnosticTask(final PythonBridge pythonBridge) {
        this.pythonBridge = pythonBridge;
    }

    @Override
    public DiagnosticsDumpElement captureDump(final boolean verbose) {
        final List<String> details = new ArrayList<>();

        // List all Python Processors and versions
        final List<PythonProcessorDetails> processorDetails = pythonBridge.getProcessorTypes();
        details.add("Python Processors Available:");
        for (final PythonProcessorDetails procDetails : processorDetails) {
            details.add(procDetails.getProcessorType() + " :: " + procDetails.getProcessorVersion());
        }

        final Map<String, Integer> counts = pythonBridge.getProcessCountsPerType();
        details.add("");
        details.add("Number of Python Processes for each Processor Type:");
        counts.forEach((key, value) -> details.add(key + " : " + value));

        if (verbose) {
            details.add("");
            details.add("Bound Object Counts:");

            final List<BoundObjectCounts> boundObjectCounts = pythonBridge.getBoundObjectCounts();
            for (final BoundObjectCounts objectCounts : boundObjectCounts) {
                details.add(objectCounts.getProcess());
                details.add(objectCounts.getProcessorType());

                // Sort the listing to show the classes with the largest numbers of entries first
                final List<Map.Entry<String, Integer>> countsList = new ArrayList<>(objectCounts.getCounts().entrySet());
                final Comparator<Entry<String, Integer>> comparator = Map.Entry.comparingByValue();
                final Comparator<Entry<String, Integer>> reversed = comparator.reversed();
                countsList.sort(reversed);

                countsList.forEach(entry -> {
                    details.add(entry.getKey() + " : " + entry.getValue());
                });

                details.add("");
            }
        }

        return new StandardDiagnosticsDumpElement("Python Bridge", details);
    }
}
