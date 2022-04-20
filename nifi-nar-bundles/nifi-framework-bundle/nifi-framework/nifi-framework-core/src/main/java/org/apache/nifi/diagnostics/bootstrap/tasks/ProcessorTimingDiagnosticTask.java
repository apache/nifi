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

import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.repository.FlowFileEvent;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.RepositoryStatusReport;
import org.apache.nifi.diagnostics.DiagnosticTask;
import org.apache.nifi.diagnostics.DiagnosticsDumpElement;
import org.apache.nifi.diagnostics.StandardDiagnosticsDumpElement;
import org.apache.nifi.processor.DataUnit;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class ProcessorTimingDiagnosticTask implements DiagnosticTask {
    private final FlowFileEventRepository eventRepo;
    private final FlowManager flowManager;

    //                                                     | Proc ID    | Proc Name  | Proc Type  | Group Name | Proc Secs | CPU Secs  | %CPU used by Proc |
    private static final String PROCESSOR_TIMING_FORMAT = "| %1$-36.36s | %2$-36.36s | %3$-36.36s | %4$-36.36s | %5$15.15s | %6$27.27s | %7$25.25s | " +
    //   Read Secs | Write Secs| Commit Sec | GC millis | MB Read    | MB Write   |
        "%8$16.16s | %9$16.16s | %10$20.20s | %11$13.13s | %12$11.11s | %13$11.11s |";

    public ProcessorTimingDiagnosticTask(final FlowFileEventRepository flowFileEventRepository, final FlowManager flowManager) {
        this.eventRepo = flowFileEventRepository;
        this.flowManager = flowManager;
    }

    @Override
    public DiagnosticsDumpElement captureDump(final boolean verbose) {
        final List<String> details = new ArrayList<>();

        final RepositoryStatusReport statusReport = eventRepo.reportTransferEvents(System.currentTimeMillis());
        final Map<String, FlowFileEvent> eventsByComponentId = statusReport.getReportEntries();

        final List<ProcessorTiming> timings = new ArrayList<>();
        eventsByComponentId.entrySet().stream()
            .map(entry -> getTiming(entry.getKey(), entry.getValue()))
            .filter(Objects::nonNull)
            .forEach(timings::add); // create ArrayList and add here instead of .collect(toList()) because arraylist allows us to sort

        // Sort based on the Processor CPU time, highest CPU usage first
        timings.sort(Comparator.comparing(ProcessorTiming::getCpuNanos).reversed());

        final DecimalFormat dataSizeFormat = new DecimalFormat("#,###,###.##");
        final DecimalFormat percentageFormat = new DecimalFormat("##.##");
        final NumberFormat secondsFormat = NumberFormat.getInstance();

        long totalCpuNanos = 0L;
        long totalProcNanos = 0L;
        long totalReadNanos = 0L;
        long totalWriteNanos = 0L;
        long totalSessionCommitNanos = 0L;
        long totalBytesRead = 0L;
        long totalBytesWritten = 0L;
        long totalGcNanos = 0L;

        // Tally totals for all timing elements
        for (final ProcessorTiming timing : timings) {
            totalCpuNanos += timing.getCpuNanos();
            totalProcNanos += timing.getProcessingNanos();
            totalReadNanos += timing.getReadNanos();
            totalWriteNanos += timing.getWriteNanos();
            totalSessionCommitNanos += timing.getSessionCommitNanos();
            totalBytesRead += timing.getBytesRead();
            totalBytesWritten += timing.getBytesWritten();
            totalGcNanos += timing.getGarbageCollectionNanos();
        }

        if (totalCpuNanos < 1) {
            details.add("No Processor Timing Diagnostic information has been gathered.");
            return new StandardDiagnosticsDumpElement("Processor Timing Diagnostics (Stats over last 5 minutes)", details);
        }

        details.add(String.format(PROCESSOR_TIMING_FORMAT, "Processor ID", "Processor Name", "Processor Type", "Process Group Name", "Processing Secs",
            "CPU Secs (% time using CPU)", "Pct CPU Time Used by Proc", "Disk Read Secs", "Disk Write Secs", "Session Commit Secs", "GC Millis", "MB Read", "MB Written"));

        for (final ProcessorTiming timing : timings) {
            final long procNanos = timing.getProcessingNanos();
            if (procNanos < 1) {
                continue;
            }

            final String cpuTime = nanosToPercentTime(timing.getCpuNanos(), procNanos, secondsFormat);
            final String cpuPct = percentageFormat.format(timing.getCpuNanos() * 100 / totalCpuNanos);
            final String readTime = nanosToPercentTime(timing.getReadNanos(), procNanos, secondsFormat);
            final String writeTime = nanosToPercentTime(timing.getWriteNanos(), procNanos, secondsFormat);
            final String commitTime = nanosToPercentTime(timing.getSessionCommitNanos(), procNanos, secondsFormat);
            final String gcTime = nanosToPercentTime(timing.getGarbageCollectionNanos(), procNanos, secondsFormat);

            final String formatted = String.format(PROCESSOR_TIMING_FORMAT, timing.getId(), timing.getName(), timing.getType(), timing.getGroupName(),
                secondsFormat.format(TimeUnit.NANOSECONDS.toSeconds(timing.getProcessingNanos())),
                cpuTime,
                cpuPct,
                readTime,
                writeTime,
                commitTime,
                gcTime,
                dataSizeFormat.format(DataUnit.B.toMB(timing.getBytesRead())),
                dataSizeFormat.format(DataUnit.B.toMB(timing.getBytesWritten())));
            details.add(formatted);
        }

        final String formatted = String.format(PROCESSOR_TIMING_FORMAT, "Total", "--", "--", "--",
            secondsFormat.format(TimeUnit.NANOSECONDS.toSeconds(totalProcNanos)),
            nanosToPercentTime(totalCpuNanos, totalProcNanos, secondsFormat),
            "100.00", // Always represents 100% of CPU time used
            nanosToPercentTime(totalReadNanos, totalProcNanos, secondsFormat),
            nanosToPercentTime(totalWriteNanos, totalProcNanos, secondsFormat),
            nanosToPercentTime(totalSessionCommitNanos, totalProcNanos, secondsFormat),
            nanosToPercentTime(totalGcNanos, totalProcNanos, secondsFormat),
            dataSizeFormat.format(DataUnit.B.toMB(totalBytesRead)),
            dataSizeFormat.format(DataUnit.B.toMB(totalBytesWritten)));
        details.add(formatted);

        final double mbReadPerSecond = DataUnit.B.toMB(totalBytesRead) / 300;
        final double mbWrittenPerSecond = DataUnit.B.toMB(totalBytesWritten) / 300;
        details.add("");
        details.add("Average MB/sec read from Content Repo in last 5 mins: " + dataSizeFormat.format(mbReadPerSecond));
        details.add("Average MB/sec written to Content Repo in last 5 mins: " + dataSizeFormat.format(mbWrittenPerSecond));

        return new StandardDiagnosticsDumpElement("Processor Timing Diagnostics (Stats over last 5 minutes)", details);
    }

    private String nanosToPercentTime(final long nanos, final long processingNanos, final NumberFormat secondsFormat) {
        return secondsFormat.format(TimeUnit.NANOSECONDS.toSeconds(nanos)) + " (" + Math.min(100, ((int) (nanos * 100 / processingNanos))) + "%)";
    }

    private ProcessorTiming getTiming(final String processorId, final FlowFileEvent flowFileEvent) {
        final ProcessorNode processorNode = flowManager.getProcessorNode(processorId);

        // Processor may be null because the event may not be for a processor, or the processor may already have been removed.
        if (processorNode == null) {
            return null;
        }

        final ProcessorTiming timing = new ProcessorTiming(processorNode, flowFileEvent);
        return timing;
    }

    private static class ProcessorTiming {
        private final String id;
        private final String name;
        private final String type;
        private final String groupName;
        private final FlowFileEvent flowFileEvent;

        public ProcessorTiming(final ProcessorNode processor, final FlowFileEvent flowFileEvent) {
            this.id = processor.getIdentifier();
            this.name = processor.getName();
            this.type = processor.getComponentType();
            this.groupName = processor.getProcessGroup().getName();
            this.flowFileEvent = flowFileEvent;
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        public String getGroupName() {
            return groupName;
        }

        public long getProcessingNanos() {
            return flowFileEvent.getProcessingNanoseconds();
        }

        public long getCpuNanos() {
            return flowFileEvent.getCpuNanoseconds();
        }

        public long getReadNanos() {
            return flowFileEvent.getContentReadNanoseconds();
        }

        public long getWriteNanos() {
            return flowFileEvent.getContentWriteNanoseconds();
        }

        public long getSessionCommitNanos() {
            return flowFileEvent.getSessionCommitNanoseconds();
        }

        public long getBytesRead() {
            return flowFileEvent.getBytesRead();
        }

        public long getBytesWritten() {
            return flowFileEvent.getBytesWritten();
        }

        public long getGarbageCollectionNanos() {
            return TimeUnit.MILLISECONDS.toNanos(flowFileEvent.getGargeCollectionMillis());
        }
    }
}
