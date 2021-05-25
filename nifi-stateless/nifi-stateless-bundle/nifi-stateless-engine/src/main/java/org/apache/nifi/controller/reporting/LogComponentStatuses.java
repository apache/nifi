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

package org.apache.nifi.controller.reporting;

import org.apache.nifi.controller.Counter;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.repository.CounterRepository;
import org.apache.nifi.controller.repository.FlowFileEvent;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LogComponentStatuses implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(LogComponentStatuses.class);
    private static final int METRIC_CACHE_SECONDS = 300; // FlowFileEvent Repository holds 300 seconds' worth of metrics/events

    private static final String PROCESSOR_LINE_FORMAT = "| %1$-30.30s | %2$-36.36s | %3$-30.30s | %4$28.28s | %5$30.30s | %6$14.14s | %714.14s | %8$28.28s |\n";
    private static final String COUNTER_LINE_FORMAT = "| %1$-36.36s | %2$-36.36s | %3$28.28s | %4$28.28s |\n";

    private final FlowFileEventRepository flowFileEventRepository;
    private final CounterRepository counterRepository;
    private final FlowManager flowManager;

    private final String processorHeader;
    private final String processorBorderLine;
    private final String counterHeader;
    private final String counterBorderLine;

    private final Map<String, Long> previousCounterValues = new ConcurrentHashMap<>();
    private volatile long lastTriggerTime = System.currentTimeMillis();

    public LogComponentStatuses(final FlowFileEventRepository flowFileEventRepository, final CounterRepository counterRepository, final FlowManager flowManager) {
        this.flowFileEventRepository = flowFileEventRepository;
        this.counterRepository = counterRepository;
        this.flowManager = flowManager;

        processorHeader = String.format(PROCESSOR_LINE_FORMAT, "Processor Name", "Processor ID", "Processor Type", "Bytes Read/sec", "Bytes Written/sec", "Tasks/sec", "Nanos/Task",
            "Percent of Processing Time");
        processorBorderLine = createLine(processorHeader);

        counterHeader = String.format(COUNTER_LINE_FORMAT, "Counter Context", "Counter Name", "Counter Value", "Increase/sec");
        counterBorderLine = createLine(counterHeader);
    }

    private String createLine(final String valueToUnderscore) {
        final StringBuilder processorBorderBuilder = new StringBuilder(valueToUnderscore.length());
        for (int i = 0; i < valueToUnderscore.length(); i++) {
            processorBorderBuilder.append('-');
        }
        return processorBorderBuilder.toString();
    }

    @Override
    public void run() {
        try {
            if (!logger.isInfoEnabled()) {
                return;
            }

            logFlowFileEvents();
            logCounters();
        } catch (final Exception e) {
            logger.error("Failed to log component statuses", e);
        }
    }

    private void logFlowFileEvents() {
        final long timestamp = System.currentTimeMillis();
        final ProcessGroup rootGroup = flowManager.getRootGroup();
        final List<ProcessorNode> allProcessors = rootGroup.findAllProcessors();

        long totalNanos = 0L;
        final List<ProcessorAndEvent> processorsAndEvents = new ArrayList<>();
        for (final ProcessorNode processorNode : allProcessors) {
            final FlowFileEvent flowFileEvent = flowFileEventRepository.reportTransferEvents(processorNode.getIdentifier(), timestamp);
            if (flowFileEvent == null) {
                continue;
            }

            processorsAndEvents.add(new ProcessorAndEvent(processorNode, flowFileEvent));
            totalNanos += flowFileEvent.getProcessingNanoseconds();
        }

        final Comparator<ProcessorAndEvent> comparator = Comparator.comparing(procAndEvent -> procAndEvent.getEvent().getProcessingNanoseconds());
        processorsAndEvents.sort(comparator.reversed());

        final StringBuilder builder = new StringBuilder();
        builder.append("Processor Statuses:\n");
        builder.append(processorBorderLine);
        builder.append("\n");
        builder.append(processorHeader);
        builder.append(processorBorderLine);
        builder.append("\n");

        for (final ProcessorAndEvent processorAndEvent : processorsAndEvents) {
            addStatus(processorAndEvent, builder, METRIC_CACHE_SECONDS, totalNanos);
        }

        builder.append(processorBorderLine);
        logger.info(builder.toString());
    }

    private void addStatus(final ProcessorAndEvent processorAndEvent, final StringBuilder builder, final int secondsInEvent, final long totalNanos) {
        final ProcessorNode processorNode = processorAndEvent.getProcessorNode();
        final FlowFileEvent flowFileEvent = processorAndEvent.getEvent();

        final long bytesReadPerSecond = flowFileEvent.getBytesRead() / secondsInEvent;
        final long bytesWrittenPerSecond = flowFileEvent.getBytesWritten() / secondsInEvent;
        final double invocations = (double) flowFileEvent.getInvocations() / (double) secondsInEvent;
        final long nanos = flowFileEvent.getProcessingNanoseconds();
        final double nanosPer = (double) nanos / invocations;
        final double nanosRatio = (double) nanos / (double) totalNanos;
        final double processingPercent = nanosRatio * 100D;
        final String processingPercentTwoDecimals = String.format("%.2f %%", processingPercent);

        final String bytesRead = FormatUtils.formatDataSize(bytesReadPerSecond);
        final String bytesWritten = FormatUtils.formatDataSize(bytesWrittenPerSecond);
        final String invocationsPerSec = String.format("%.2f", invocations);
        final String nanosPerInvocation = String.format("%.2f", nanosPer);

        builder.append(String.format(PROCESSOR_LINE_FORMAT,
            processorNode.getName(),
            processorNode.getIdentifier(),
            processorNode.getComponentType(),
            bytesRead,
            bytesWritten,
            invocationsPerSec,
            nanosPerInvocation,
            processingPercentTwoDecimals));
    }

    private void logCounters() {
        final StringBuilder builder = new StringBuilder();
        builder.append("Counters:\n");
        builder.append(counterBorderLine);
        builder.append("\n");
        builder.append(counterHeader);
        builder.append(counterBorderLine);
        builder.append("\n");

        final long now = System.currentTimeMillis();
        final long millisSinceLastTrigger = now - lastTriggerTime;
        final double secondsSinceLastTrigger = (double) millisSinceLastTrigger / 1000D;
        lastTriggerTime = now;

        final List<Counter> counters = counterRepository.getCounters();
        counters.sort(Comparator.comparing(Counter::getContext).thenComparing(Counter::getName));

        for (final Counter counter : counters) {
            final String counterId = counter.getIdentifier();
            final long lastValue = previousCounterValues.getOrDefault(counterId, 0L);
            previousCounterValues.put(counterId, counter.getValue());
            final long increaseSinceLast = counter.getValue() - lastValue;
            final double increasePerSecond = (double) increaseSinceLast / secondsSinceLastTrigger;
            final String increase = String.format("%.2f", increasePerSecond);

            builder.append(String.format(COUNTER_LINE_FORMAT, counter.getContext(), counter.getName(), counter.getValue(), increase));
        }

        builder.append(counterBorderLine);
        logger.info(builder.toString());
    }

    private static class ProcessorAndEvent {
        private final ProcessorNode processorNode;
        private final FlowFileEvent event;

        public ProcessorAndEvent(final ProcessorNode processorNode, final FlowFileEvent event) {
            this.processorNode = processorNode;
            this.event = event;
        }

        public ProcessorNode getProcessorNode() {
            return processorNode;
        }

        public FlowFileEvent getEvent() {
            return event;
        }
    }
}
