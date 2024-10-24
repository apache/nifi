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

package org.apache.nifi.controller.tasks;

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.ConnectionUtils;
import org.apache.nifi.connectable.ConnectionUtils.FlowFileCloneResult;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.Triggerable;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileEvent;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.RepositoryRecord;
import org.apache.nifi.controller.repository.RepositoryRecordType;
import org.apache.nifi.controller.repository.StandardFlowFileRecord;
import org.apache.nifi.controller.repository.StandardRepositoryRecord;
import org.apache.nifi.controller.repository.metrics.StandardFlowFileEvent;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.StatelessGroupNode;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.exception.TerminatedTaskException;
import org.apache.nifi.provenance.ProvenanceEventBuilder;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.stateless.flow.DataflowTrigger;
import org.apache.nifi.stateless.flow.DataflowTriggerContext;
import org.apache.nifi.stateless.flow.FailurePortEncounteredException;
import org.apache.nifi.stateless.flow.FlowFileSupplier;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.apache.nifi.stateless.flow.TriggerResult;
import org.apache.nifi.stateless.repository.StatelessProvenanceRepository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class StatelessFlowTask {
    private static final Set<ProvenanceEventType> eventTypesToKeepOnFailure = EnumSet.of(ProvenanceEventType.SEND, ProvenanceEventType.REMOTE_INVOCATION);
    private final StatelessGroupNode statelessGroupNode;
    private final StatelessDataflow flow;
    private final FlowFileRepository nifiFlowFileRepository;
    private final ProvenanceEventRepository nifiProvenanceEventRepository;
    private final ContentRepository nifiContentRepository;
    private final FlowFileEventRepository flowFileEventRepository;
    private final ComponentLog logger;
    private final Map<String, Port> outputPorts;
    private final long timeoutMillis;
    private final boolean allowBatch;

    // State that is updated during invocation - these variables are guarded by synchronized block
    private List<FlowFileCloneResult> cloneResults;
    private List<RepositoryRecord> outputRepositoryRecords;
    private List<ProvenanceEventRecord> cloneProvenanceEvents;


    private StatelessFlowTask(final Builder builder) {
        this.statelessGroupNode = builder.statelessGroupNode;
        this.nifiFlowFileRepository = builder.flowFileRepository;
        this.nifiContentRepository = builder.contentRepository;
        this.nifiProvenanceEventRepository = builder.provenanceEventRepository;
        this.flowFileEventRepository = builder.flowFileEventRepository;
        this.flow = builder.statelessFlow;
        this.allowBatch = isAllowBatch(statelessGroupNode.getProcessGroup());
        this.logger = builder.logger;
        this.timeoutMillis = builder.timeoutMillis;

        final ProcessGroup processGroup = statelessGroupNode.getProcessGroup();

        outputPorts = new HashMap<>();
        for (final Port outputPort : processGroup.getOutputPorts()) {
            outputPorts.put(outputPort.getName(), outputPort);
        }
    }


    private boolean isAllowBatch(final ProcessGroup group) {
        // We allow batch only if there are no processors that use @TriggerSerially.
        // If any Processor does require being triggered serially, it may indicate that it is not allowed
        // to perform multiple invocations without first committing its session. In this case, we cannot
        // use multiple threads, and we cannot use batch processing.
        return group.findAllProcessors().stream()
            .noneMatch(this::isPreventBatch);
    }

    private boolean isPreventBatch(final ProcessorNode procNode) {
        if (procNode.isTriggeredSerially()) {
            return true;
        }

        if (procNode.hasIncomingConnection()) {
            return false;
        }
        if (!isRunAsFastAsPossible(procNode)) {
            return true;
        }

        return false;
    }

    private boolean isRunAsFastAsPossible(final ProcessorNode procNode) {
        final SchedulingStrategy schedulingStrategy = procNode.getSchedulingStrategy();
        if (schedulingStrategy != SchedulingStrategy.TIMER_DRIVEN) {
            return false;
        }

        if (procNode.getSchedulingPeriod(TimeUnit.NANOSECONDS) > Triggerable.MINIMUM_SCHEDULING_NANOS) {
            return false;
        }

        return true;
    }

    public void shutdown() {
        this.flow.shutdown(false, true);
    }

    private boolean isAbort() {
        final ScheduledState desiredState = statelessGroupNode.getDesiredState();
        return desiredState != ScheduledState.RUNNING && desiredState != ScheduledState.RUN_ONCE;
    }


    public synchronized void trigger() {
        final long startTime = System.currentTimeMillis();
        final long endTime = startTime + 100L;

        if (allowBatch) {
            logger.debug("Will run in batch mode for 100 milliseconds until {}", endTime);
        }

        final List<Invocation> allInvocations = new ArrayList<>();
        final List<Invocation> successfulInvocations = new ArrayList<>();

        final ProvenanceEventRepository statelessProvRepo = new StatelessProvenanceRepository(10_000);

        try {
            int invocationCount = 0;
            while ((invocationCount == 0 || allowBatch) && System.currentTimeMillis() < endTime) {
                invocationCount++;

                final Invocation invocation = new Invocation();
                final FlowFileSupplier flowFileSupplier = new BridgingFlowFileSupplier(invocation);
                final DataflowTriggerContext triggerContext = new StatelessFlowTaskTriggerContext(flowFileSupplier, statelessProvRepo);

                final TriggerResult triggerResult = triggerFlow(triggerContext);
                invocation.setTriggerResult(triggerResult);

                allInvocations.add(invocation);

                if (triggerResult.isSuccessful()) {
                    successfulInvocations.add(invocation);

                    // If we pulled in more than 1 FlowFile for this invocation, do not trigger again.
                    // We do this because most of the time when multiple FlowFiles are pulled in, it's for a merge, etc.
                    // and in that case, there may well not be enough FlowFiles for another invocation to succeed. We don't
                    // want to hold up this invocation until the flow times out, so we just stop triggering.
                    if (invocation.getPolledFlowFiles().size() > 1) {
                        break;
                    }
                } else {
                    logger.debug("Failed to trigger", triggerResult.getFailureCause().orElse(null));
                    fail(invocation, statelessProvRepo);
                    break;
                }
            }

            logger.debug("Finished triggering");
        } finally {
            try {
                completeInvocations(successfulInvocations, statelessProvRepo);
            } catch (final Exception e) {
                logger.error("Failed to complete Stateless Flow", e);
                statelessGroupNode.yield();
                fail(successfulInvocations, statelessProvRepo, e);
            }

            logger.debug("Acknowledging FlowFiles from {} invocations", allInvocations.size());
            for (final Invocation invocation : allInvocations) {
                for (final PolledFlowFile polledFlowFile : invocation.getPolledFlowFiles()) {
                    polledFlowFile.getOriginalQueue().acknowledge(polledFlowFile.getInputFlowFile());
                }
            }
        }
    }


    private void fail(final List<Invocation> invocations, final ProvenanceEventRepository statelessProvRepo, final Throwable cause) {
        invocations.forEach(invocation -> fail(invocation, statelessProvRepo, cause));
    }

    private void fail(final Invocation invocation, final ProvenanceEventRepository statelessProvRepo) {
        final Throwable cause;
        if (invocation.getTriggerResult().isCanceled()) {
            cause = new TerminatedTaskException();
        } else {
            cause = invocation.getTriggerResult().getFailureCause().orElse(null);
        }

        fail(invocation, statelessProvRepo, cause);
    }

    private void fail(final Invocation invocation, final ProvenanceEventRepository statelessProvRepo, final Throwable cause) {
        final Port destinationPort = getDestinationPort(cause);

        try {
            failInvocation(invocation, statelessProvRepo, destinationPort, cause);
        } catch (final Exception e) {
            if (cause != null) {
                cause.addSuppressed(e);
            }

            logger.error("Failed to trigger Stateless Flow and failed to properly handle failure", cause);
        }
    }

    private Port getDestinationPort(final Throwable failureCause) {
        if (!(failureCause instanceof final FailurePortEncounteredException fpee)) {
            return null;
        }

        final Port port = this.outputPorts.get(fpee.getPortName());
        if (port == null) {
            logger.error("FlowFile was routed to Failure Port {} but no such port exists in the dataflow", fpee.getPortName());
        }

        return port;
    }

    private TriggerResult triggerFlow(final DataflowTriggerContext triggerContext) {
        final DataflowTrigger trigger = flow.trigger(triggerContext);

        try {
            final Optional<TriggerResult> optionalResult = trigger.getResult(timeoutMillis, TimeUnit.MILLISECONDS);
            if (optionalResult.isEmpty()) {
                trigger.cancel();
                return trigger.getResult(5, TimeUnit.SECONDS)
                    .orElseThrow(() -> new ProcessException("Stateless Flow " + this + " timed out and failed to cancel within the allotted amount of time"));
            }

            return optionalResult.get();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }


    private void completeInvocations(final List<Invocation> invocations, final ProvenanceEventRepository statelessProvRepo) throws IOException {
        logger.debug("Completing transactions from {} invocations", invocations.size());
        if (invocations.isEmpty()) {
            return;
        }

        resetState();

        // Validate that the outputs are valid before we do anything else.
        for (final Invocation invocation : invocations) {
            validateDestinations(invocation.getTriggerResult());
        }

        // Drop the input FlowFiles from the FlowFile Repository. This will also ensure that the Claimant Count
        // gets updated for the input FlowFiles and that the claims are properly destroyed if necessary.
        for (final Invocation invocation : invocations) {
            dropInputFlowFiles(invocation);
        }

        // Determine where the FlowFiles are going and create the appropriate RepositoryRecords, populating
        // this.outputRepositoryRecords and this.cloneResults
        for (final Invocation invocation : invocations) {
            createOutputRecords(invocation.getTriggerResult().getOutputFlowFiles());
        }

        // Update the claimant counts based on output FlowFiles.
        updateClaimantCounts();

        // Update the FlowFile Repository to reflect that the input FlowFiles were dropped and the output FlowFiles were created.
        try {
            updateFlowFileRepository();
        } catch (final Exception e) {
            throw new IOException("Failed to update FlowFile Repository after triggering " + this, e);
        }

        updateProvenanceRepository(statelessProvRepo, event -> true);

        // Acknowledge the invocations so that the sessions can be committed
        for (final Invocation invocation : invocations) {
            acknowledge(invocation);
        }

        updateEventRepository(invocations);

        // Distribute FlowFiles to the outbound queues
        distributeFlowFiles();
    }

    void resetState() {
        cloneResults = new ArrayList<>();
        outputRepositoryRecords = new ArrayList<>();
        cloneProvenanceEvents = new ArrayList<>();
    }

    private void failInvocation(final Invocation invocation, final ProvenanceEventRepository statelessProvRepo, final Port destinationPort, final Throwable cause) throws IOException {
        final List<PolledFlowFile> inputFlowFiles = invocation.getPolledFlowFiles();

        boolean stopped = false;
        if (cause instanceof TerminatedTaskException) {
            final String input = inputFlowFiles.isEmpty() ? "no input FlowFile" : inputFlowFiles.toString();

            // A TerminatedTaskException will happen for 2 reasons: the group was stopped, or it timed out.
            // If it was stopped, just log at an INFO level, as this is expected. If it timed out, log an error.
            final ScheduledState desiredState = this.statelessGroupNode.getDesiredState();
            if (desiredState == ScheduledState.STOPPED) {
                logger.info("Stateless Flow canceled while running with input {}", input);
                stopped = true;
            } else {
                logger.error("Stateless Flow timed out while running with input {}", input);
            }
        } else {
            final String input = inputFlowFiles.isEmpty() ? "with no input FlowFile" : " for input " + inputFlowFiles;
            logger.error("Failed to trigger Stateless Flow {}", input, cause);
        }

        resetState();

        // While the successful path calls dropInputFlowFile, we do not do this in the failure path because on failure
        // the Stateless Flow will be purged, and this will handle dropping all FlowFiles that are in the flow.

        // Determine where the FlowFiles are going and create the appropriate RepositoryRecords, populating
        // this.outputRepositoryRecords and this.cloneResults
        if (!inputFlowFiles.isEmpty()) {
            if (destinationPort == null) {
                // There is no destination port. Create a penalized FlowFile and place it back on its original queue.
                // Do not penalize FlowFiles if the group was stopped, though. That is not a true failure.
                for (final PolledFlowFile polledFlowFile : inputFlowFiles) {
                    if (stopped) {
                        polledFlowFile.getOriginalQueue().put(polledFlowFile.getInputFlowFile());
                    } else {
                        final FlowFileRecord newFile = new StandardFlowFileRecord.Builder()
                            .fromFlowFile(polledFlowFile.getInputFlowFile())
                            .penaltyExpirationTime(System.currentTimeMillis() + 30_000L)
                            .build();

                        polledFlowFile.getOriginalQueue().put(newFile);
                    }
                }
            } else {
                dropInputFlowFiles(invocation);

                final Map<String, List<FlowFile>> outputRecords = Collections.singletonMap(destinationPort.getName(),
                    inputFlowFiles.stream().map(PolledFlowFile::getInputFlowFile).collect(Collectors.toList()));

                createOutputRecords(outputRecords);

                // Update the claimant counts based on output FlowFiles.
                updateClaimantCounts();
            }
        }

        // Update the FlowFile Repository to reflect that the input FlowFiles were dropped and the output FlowFiles were created.
        try {
            updateFlowFileRepository();
        } catch (final Exception e) {
            throw new IOException("Failed to update FlowFile Repository after triggering " + this, e);
        }

        updateProvenanceRepository(statelessProvRepo, event -> eventTypesToKeepOnFailure.contains(event.getEventType()));

        // Acknowledge the invocations so that the sessions can be committed
        abort(invocation, cause);

        updateEventRepository(Collections.singletonList(invocation));

        // Distribute FlowFiles to the outbound queues
        distributeFlowFiles();
    }

    private void validateDestinations(final TriggerResult result) {
        final Map<String, List<FlowFile>> outputFlowFiles = result.getOutputFlowFiles();

        // Validate the output to ensure that all given port names are valid
        for (final Map.Entry<String, List<FlowFile>> entry : outputFlowFiles.entrySet()) {
            final String portName = entry.getKey();
            final Port outputPort = outputPorts.get(portName);
            if (outputPort == null) {
                logger.error("Transferred FlowFile to Output Port {} but no port is known with that name", portName);
                throw new IllegalStateException("FlowFile was transferred to nonexistent Port " + portName);
            }
        }
    }

    // Visible for testing
    void dropInputFlowFiles(final Invocation invocation) {
        for (final PolledFlowFile polledFlowFile : invocation.getPolledFlowFiles()) {
            final FlowFileRecord inputFlowFile = polledFlowFile.getInputFlowFile();
            if (inputFlowFile == null) {
                continue;
            }

            final StandardRepositoryRecord repoRecord = new StandardRepositoryRecord(polledFlowFile.getOriginalQueue(), polledFlowFile.getInputFlowFile());
            repoRecord.markForDelete();
            outputRepositoryRecords.add(repoRecord);
        }
    }


    List<RepositoryRecord> getOutputRepositoryRecords() {
        return outputRepositoryRecords;
    }

    public List<FlowFileCloneResult> getCloneResults() {
        return cloneResults;
    }

    public List<ProvenanceEventRecord> getCloneProvenanceEvents() {
        return cloneProvenanceEvents;
    }

    void createOutputRecords(final Map<String, List<FlowFile>> outputFlowFiles) {
        for (final Map.Entry<String, List<FlowFile>> entry : outputFlowFiles.entrySet()) {
            final String portName = entry.getKey();
            final Port outputPort = outputPorts.get(portName);

            final List<FlowFileRecord> portFlowFiles = (List) entry.getValue();
            final Set<Connection> outputConnections = outputPort.getConnections();
            for (final FlowFileRecord outputFlowFile : portFlowFiles) {
                final FlowFileCloneResult cloneResult = ConnectionUtils.clone(outputFlowFile, outputConnections,
                    nifiFlowFileRepository, null);
                cloneResults.add(cloneResult);

                final List<RepositoryRecord> repoRecords = cloneResult.getRepositoryRecords();
                outputRepositoryRecords.addAll(repoRecords);

                // If we generated any clones, create provenance events for them.
                createCloneProvenanceEvent(outputFlowFile, repoRecords, outputPort).ifPresent(cloneProvenanceEvents::add);
            }
        }
    }

    void updateProvenanceRepository(final ProvenanceEventRepository statelessRepo, final Predicate<ProvenanceEventRecord> eventFilter) {
        long firstProvEventId = 0;

        if (!cloneProvenanceEvents.isEmpty()) {
            nifiProvenanceEventRepository.registerEvents(cloneProvenanceEvents);
        }

        while (true) {
            try {
                final List<ProvenanceEventRecord> statelessProvEvents = statelessRepo.getEvents(firstProvEventId, 1000);
                if (statelessProvEvents.isEmpty()) {
                    return;
                }

                // We don't want to use the ProvenanceEventRecord objects directly because they already have their IDs populated.
                // Instead, we will create a new Provenance Event from the Stateless Event, but the #fromEvent method does not
                // copy the Event ID.
                final List<ProvenanceEventRecord> provenanceEvents = new ArrayList<>();
                for (final ProvenanceEventRecord eventRecord : statelessProvEvents) {
                    if (!eventFilter.test(eventRecord)) {
                        continue;
                    }

                    provenanceEvents.add(new StandardProvenanceEventRecord.Builder()
                        .fromEvent(eventRecord)
                        .build());
                }

                nifiProvenanceEventRepository.registerEvents(provenanceEvents);

                if (provenanceEvents.size() == 1000) {
                    firstProvEventId += 1000;
                } else {
                    break;
                }
            } catch (final IOException e) {
                logger.warn("Failed to obtain Provenance Events from Stateless Dataflow. These events will not be added to the NiFi Provenance Repository", e);
            }
        }
    }

    void updateClaimantCounts() {
        // Update Claimant Counts. The Stateless engine will decrement the Claimant Count for each output FlowFile, as it no longer is responsible for
        // the FlowFiles and their content. Now, we are taking ownership of them and need to increment the Claimant Count for each FlowFile.
        // But we also no longer have the input FlowFile in its queue. So we need to decrement its claimant count. We could try to be more clever and
        // only decrement the claimant count if the input FlowFile was not transferred out, but simply incrementing for each output FlowFiles and decrementing
        // for the input FlowFile is simpler and will work just as well.
        for (final RepositoryRecord outputRepoRecord : outputRepositoryRecords) {
            if (outputRepoRecord.getType() != RepositoryRecordType.DELETE) {
                nifiContentRepository.incrementClaimaintCount(outputRepoRecord.getCurrentClaim());
            }
        }
    }

    private void updateFlowFileRepository() throws IOException {
        // Update the FlowFile repository
        nifiFlowFileRepository.updateRepository(outputRepositoryRecords);
    }

    private void acknowledge(final Invocation invocation) {
        invocation.getTriggerResult().acknowledge();
    }

    private void abort(final Invocation invocation, final Throwable cause) {
        invocation.getTriggerResult().abort(cause);
    }

    private void distributeFlowFiles() {
        int enqueued = 0;
        for (final FlowFileCloneResult result : cloneResults) {
            enqueued += result.distributeFlowFiles();
        }

        logger.debug("Distributed {} FlowFiles to output queues", enqueued);
    }

    void updateEventRepository(final List<Invocation> invocations) {
        final Map<String, StandardFlowFileEvent> eventsByComponentId = new HashMap<>();

        for (final Invocation invocation : invocations) {
            final List<PolledFlowFile> polledFlowFiles = invocation.getPolledFlowFiles();
            for (final PolledFlowFile polledFlowFile : polledFlowFiles) {
                final long bytes = polledFlowFile.getInputFlowFile().getSize();

                final int numOutputConnections = polledFlowFile.getInputPort().getConnections().size();
                final StandardFlowFileEvent flowFileEvent = new StandardFlowFileEvent();
                flowFileEvent.setFlowFilesIn(1);
                flowFileEvent.setFlowFilesOut(numOutputConnections);
                flowFileEvent.setContentSizeIn(bytes);
                flowFileEvent.setContentSizeOut(numOutputConnections * bytes);

                final StandardFlowFileEvent cumulativeEvent = eventsByComponentId.computeIfAbsent(polledFlowFile.getInputPort().getIdentifier(), key -> new StandardFlowFileEvent());
                cumulativeEvent.add(flowFileEvent);
            }

            final Map<String, List<FlowFile>> outputFlowFiles = invocation.getTriggerResult().getOutputFlowFiles();
            for (final Map.Entry<String, List<FlowFile>> entry : outputFlowFiles.entrySet()) {
                final String portName = entry.getKey();
                final List<FlowFile> flowFiles = entry.getValue();

                int flowFileCount = flowFiles.size();
                long byteCount = 0L;
                for (final FlowFile flowFile : flowFiles) {
                    byteCount += flowFile.getSize();
                }

                final Port port = outputPorts.get(portName);
                final int outputConnectionCount = port.getConnections().size();

                final StandardFlowFileEvent flowFileEvent = new StandardFlowFileEvent();
                flowFileEvent.setFlowFilesIn(flowFileCount);
                flowFileEvent.setFlowFilesOut(flowFileCount * outputConnectionCount);
                flowFileEvent.setContentSizeIn(byteCount);
                flowFileEvent.setContentSizeOut(byteCount * outputConnectionCount);

                final StandardFlowFileEvent cumulativeEvent = eventsByComponentId.computeIfAbsent(port.getIdentifier(), key -> new StandardFlowFileEvent());
                cumulativeEvent.add(flowFileEvent);
            }
        }

        for (final Map.Entry<String, StandardFlowFileEvent> entry : eventsByComponentId.entrySet()) {
            final String componentId = entry.getKey();
            final FlowFileEvent event = entry.getValue();

            try {
                flowFileEventRepository.updateRepository(event, componentId);
            } catch (final Exception e) {
                logger.warn("Failed to update FlowFile Event Repository", e);
            }
        }
    }


    private void expireRecords(final FlowFileQueue sourceQueue, final Set<FlowFileRecord> expiredRecords) throws IOException {
        if (expiredRecords.isEmpty()) {
            return;
        }

        final List<RepositoryRecord> repositoryRecords = new ArrayList<>();

        final long time = System.currentTimeMillis();
        final String expirationDetails = "Expiration Threshold = " + sourceQueue.getFlowFileExpiration();

        for (final FlowFileRecord expired : expiredRecords) {
            final StandardRepositoryRecord record = new StandardRepositoryRecord(sourceQueue, expired);
            record.markForDelete();
            repositoryRecords.add(record);
        }

        // Create an Iterable<ProvenanceEventRecord> instead of creating a List<ProvenanceEventRecord> in order to avoid
        // the heap consumption, since this can correlate to a lot of Provenance Events.
        final Iterable<ProvenanceEventRecord> provenanceEventIterable = new Iterable<>() {
            @Override
            public Iterator<ProvenanceEventRecord> iterator() {
                final Iterator<FlowFileRecord> expiredItr = expiredRecords.iterator();
                return new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return expiredItr.hasNext();
                    }

                    @Override
                    public ProvenanceEventRecord next() {
                        final FlowFileRecord expired = expiredItr.next();
                        final ProvenanceEventRecord provenanceEvent = new StandardProvenanceEventRecord.Builder()
                            .fromFlowFile(expired)
                            .setEventTime(time)
                            .setEventType(ProvenanceEventType.EXPIRE)
                            .setDetails(expirationDetails)
                            .setComponentId(sourceQueue.getIdentifier())
                            .setComponentType("Connection")
                            .build();

                        return provenanceEvent;
                    }
                };
            }
        };

        nifiFlowFileRepository.updateRepository(repositoryRecords);
        nifiProvenanceEventRepository.registerEvents(provenanceEventIterable);

        expiredRecords.clear();
    }

    private Optional<ProvenanceEventRecord> createCloneProvenanceEvent(final FlowFileRecord outputFlowFile, final List<RepositoryRecord> cloneRecords, final Port outputPort) {
        if (outputFlowFile == null || cloneRecords == null || cloneRecords.size() < 2) {
            return Optional.empty();
        }

        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder()
            .setEventType(ProvenanceEventType.CLONE)
            .fromFlowFile(outputFlowFile)
            .setLineageStartDate(outputFlowFile.getLineageStartDate())
            .setComponentId(outputPort.getIdentifier())
            .setComponentType("Output Port")
            .addParentFlowFile(outputFlowFile);

        for (final RepositoryRecord clone : cloneRecords) {
            final FlowFileRecord childFlowFile = clone.getCurrent();
            if (outputFlowFile.equals(childFlowFile)) {
                continue;
            }

            builder.addChildFlowFile(childFlowFile);
        }

        final ProvenanceEventRecord cloneEvent = builder.build();
        return Optional.of(cloneEvent);
    }

    @Override
    public String toString() {
        return "StatelessFlowTask[Group=" + statelessGroupNode.getProcessGroup() + "]";
    }


    static class PolledFlowFile {
        private final FlowFileRecord inputFlowFile;
        private final FlowFileQueue originalQueue;
        private final Port inputPort;

        public PolledFlowFile(final FlowFileRecord inputFlowFile, final FlowFileQueue originalQueue, final Port inputPort) {
            this.inputFlowFile = inputFlowFile;
            this.originalQueue = originalQueue;
            this.inputPort = inputPort;
        }

        public FlowFileRecord getInputFlowFile() {
            return inputFlowFile;
        }

        public FlowFileQueue getOriginalQueue() {
            return originalQueue;
        }

        public Port getInputPort() {
            return inputPort;
        }

        @Override
        public String toString() {
            return inputFlowFile.toString();
        }
    }

    static class Invocation {
        private List<PolledFlowFile> polledFlowFiles;
        private TriggerResult triggerResult;

        public void setTriggerResult(final TriggerResult triggerResult) {
            this.triggerResult = triggerResult;
        }

        public List<PolledFlowFile> getPolledFlowFiles() {
            if (polledFlowFiles == null) {
                return List.of();
            }

            return polledFlowFiles;
        }

        public TriggerResult getTriggerResult() {
            return triggerResult;
        }

        public void addPolledFlowFile(final PolledFlowFile polledFlowFile) {
            if (polledFlowFile == null) {
                return;
            }

            if (polledFlowFiles == null) {
                polledFlowFiles = new ArrayList<>();
            }
            polledFlowFiles.add(polledFlowFile);
        }
    }

    public static class Builder {
        private StatelessGroupNode statelessGroupNode;
        private FlowFileRepository flowFileRepository;
        private ContentRepository contentRepository;
        private ProvenanceEventRepository provenanceEventRepository;
        private FlowFileEventRepository flowFileEventRepository;
        private StatelessDataflow statelessFlow;
        private long timeoutMillis = TimeUnit.MINUTES.toMillis(1);
        private ComponentLog logger;

        public Builder statelessGroupNode(final StatelessGroupNode statelessGroupNode) {
            this.statelessGroupNode = statelessGroupNode;
            return this;
        }

        public Builder nifiFlowFileRepository(final FlowFileRepository flowFileRepository) {
            this.flowFileRepository = flowFileRepository;
            return this;
        }

        public Builder nifiContentRepository(final ContentRepository contentRepository) {
            this.contentRepository = contentRepository;
            return this;
        }

        public Builder nifiProvenanceRepository(final ProvenanceEventRepository provenanceEventRepository) {
            this.provenanceEventRepository = provenanceEventRepository;
            return this;
        }

        public Builder flowFileEventRepository(final FlowFileEventRepository flowFileEventRepository) {
            this.flowFileEventRepository = flowFileEventRepository;
            return this;
        }

        public Builder statelessFlow(final StatelessDataflow statelessFlow) {
            this.statelessFlow = statelessFlow;
            return this;
        }

        public Builder timeout(final long value, final TimeUnit unit) {
            final long millis = unit.toMillis(value);
            this.timeoutMillis = Math.max(millis, 1L);
            return this;
        }

        public Builder logger(final ComponentLog logger) {
            this.logger = logger;
            return this;
        }

        public StatelessFlowTask build() {
            return new StatelessFlowTask(this);
        }
    }


    /**
     * A FlowFileSupplier that bridges between the running NiFi instance and the Stateless Engine.
     */
    private class BridgingFlowFileSupplier implements FlowFileSupplier {
        private final Map<String, Port> portsByName;

        private final Set<FlowFileRecord> expiredRecords = new HashSet<>();
        private final Invocation invocation;
        private int zeroFlowFileInvocations = 0;

        public BridgingFlowFileSupplier(final Invocation invocation) {
            this.invocation = invocation;

            final Set<Port> inputPorts = statelessGroupNode.getProcessGroup().getInputPorts();
            portsByName = inputPorts.stream()
                .collect(Collectors.toMap(Port::getName, port -> port));
        }


        @Override
        public Optional<FlowFile> getFlowFile(final String portName) {
            final Port port = portsByName.get(portName);
            if (port == null) {
                return Optional.empty();
            }

            for (final Connection sourceConnection : port.getIncomingConnections()) {
                final FlowFileQueue sourceQueue = sourceConnection.getFlowFileQueue();
                final FlowFileRecord flowFile = sourceQueue.poll(expiredRecords);

                if (!expiredRecords.isEmpty()) {
                    try {
                        expireRecords(sourceQueue, expiredRecords);
                    } catch (final Exception e) {
                        logger.error("Failed to expire FlowFile Records when consuming from input queue {}", sourceQueue, e);
                    }

                    expiredRecords.clear();
                }

                if (flowFile != null) {
                    zeroFlowFileInvocations = 0;

                    // We need to increment the Content Claim for the FlowFile because when we complete the flow, for any FlowFile queued
                    // up in an Output Port, the Stateless Engine will decrement the claimant count because it no longer owns the FlowFile.
                    // Incrementing it here ensures that the claimant count is properly counted.
                    nifiContentRepository.incrementClaimaintCount(flowFile.getContentClaim());

                    invocation.addPolledFlowFile(new PolledFlowFile(flowFile, sourceQueue, port));
                    return Optional.of(flowFile);
                }
            }

            // In order to avoid overwhelming the CPU in an unproductive loop, since we have no data we will yield for 10 milliseconds.
            // We do not do this for the first invocation, however, as this is only helpful for flows that require multiple FlowFiles.
            if (++zeroFlowFileInvocations > 1) {
                final long yieldMillis = statelessGroupNode.getBoredYieldDuration(TimeUnit.MILLISECONDS);
                try {
                    Thread.sleep(yieldMillis);
                } catch (final InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }

            return Optional.empty();
        }
    }

    private class StatelessFlowTaskTriggerContext implements DataflowTriggerContext {
        private final FlowFileSupplier flowFileSupplier;
        private final ProvenanceEventRepository statelessProvRepo;

        public StatelessFlowTaskTriggerContext(final FlowFileSupplier flowFileSupplier, final ProvenanceEventRepository statelessProvRepo) {
            this.flowFileSupplier = flowFileSupplier;
            this.statelessProvRepo = statelessProvRepo;
        }

        @Override
        public boolean isAbort() {
            return StatelessFlowTask.this.isAbort();
        }

        @Override
        public FlowFileSupplier getFlowFileSupplier() {
            return flowFileSupplier;
        }

        @Override
        public ProvenanceEventRepository getProvenanceEventRepository() {
            return statelessProvRepo;
        }
    }
}
