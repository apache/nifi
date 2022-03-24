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
import org.apache.nifi.util.FormatUtils;

import java.io.File;
import java.lang.management.ClassLoadingMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class JVMDiagnosticTask implements DiagnosticTask {
    @Override
    public DiagnosticsDumpElement captureDump(final boolean verbose) {
        final MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        final ClassLoadingMXBean classLoading = ManagementFactory.getClassLoadingMXBean();
        final MemoryUsage heap = memory.getHeapMemoryUsage();
        final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        final RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();

        final List<String> details = new ArrayList<>();
        final NumberFormat numberFormat = NumberFormat.getInstance();

        details.add("Total Thread Count: " + numberFormat.format(threads.getThreadCount()));
        details.add("Daemon Thread Count: " + numberFormat.format(threads.getDaemonThreadCount()));

        details.add("Max Heap: " + FormatUtils.formatDataSize(heap.getMax()));
        details.add("Heap Used: " + FormatUtils.formatDataSize(heap.getUsed()));
        details.add("Heap Committed: " + FormatUtils.formatDataSize(heap.getCommitted()));

        details.add("JVM Uptime: " + FormatUtils.formatHoursMinutesSeconds(runtime.getUptime(), TimeUnit.MILLISECONDS));

        details.add("JVM Spec Name: " + runtime.getSpecName());
        details.add("JVM Spec Vendor: " + runtime.getSpecVendor());
        details.add("JVM Spec Version: " + runtime.getSpecVersion());

        details.add("JVM Vendor: " + runtime.getVmVendor());
        details.add("JVM Version: " + runtime.getVmVersion());
        details.add("Classes Loaded: " + numberFormat.format(classLoading.getLoadedClassCount()));
        details.add("Classes Loaded Since Start: " + numberFormat.format(classLoading.getTotalLoadedClassCount()));
        details.add("Working Directory: " + new File(".").getAbsolutePath());

        return new StandardDiagnosticsDumpElement("Java Virtual Machine", details);
    }
}
