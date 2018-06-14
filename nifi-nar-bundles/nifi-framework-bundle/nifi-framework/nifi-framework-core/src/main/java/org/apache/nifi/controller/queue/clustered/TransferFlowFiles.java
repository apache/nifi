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

package org.apache.nifi.controller.queue.clustered;

import org.apache.nifi.controller.queue.BlockingSwappablePriorityQueue;
import org.apache.nifi.controller.queue.LoadBalancedFlowFileQueue;
import org.apache.nifi.controller.queue.clustered.client.LoadBalanceClient;
import org.apache.nifi.controller.queue.clustered.client.LoadBalanceTransaction;
import org.apache.nifi.controller.repository.ContentNotFoundException;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.RepositoryRecord;
import org.apache.nifi.controller.repository.StandardRepositoryRecord;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.provenance.ProvenanceEventBuilder;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.reporting.Severity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TransferFlowFiles implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(TransferFlowFiles.class);
    private static final long maxPollMillis = 10L;

    private final BlockingSwappablePriorityQueue queue;
    private final LoadBalancedFlowFileQueue flowFileQueue;
    private final LoadBalanceClient client;
    private volatile boolean stopped = false;

    private final EventReporter eventReporter;
    private final FlowFileRepository flowFileRepo;
    private final ProvenanceEventRepository provRepo;
    private final ContentRepository contentRepo;
    private final FlowFileContentAccess flowFileAccess;
    private final TransferFailureDestination failureDestination;

    public TransferFlowFiles(final BlockingSwappablePriorityQueue queue, final LoadBalancedFlowFileQueue flowFileQueue, final LoadBalanceClient client,
                             final TransferFailureDestination failureDestination, final EventReporter eventReporter, final FlowFileRepository flowFileRepo, final ProvenanceEventRepository provRepo,
                             final ContentRepository contentRepo) {

        this.queue = queue;
        this.flowFileQueue = flowFileQueue;
        this.client = client;
        this.eventReporter = eventReporter;
        this.flowFileRepo = flowFileRepo;
        this.provRepo = provRepo;
        this.contentRepo = contentRepo;
        this.flowFileAccess = new ContentRepositoryFlowFileAccess(contentRepo);
        this.failureDestination = failureDestination;
    }

    @Override
    public void run() {
        final Set<FlowFileRecord> expiredRecords = new HashSet<>();
        final List<RepositoryRecord> abortedRecords = new ArrayList<>();
        final long expirationMillis = flowFileQueue.getFlowFileExpiration(TimeUnit.MILLISECONDS);

        while (!isStopped()) {
            try {
                transferFlowFiles(expiredRecords, abortedRecords, expirationMillis);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.debug(this + " Interrupted while waiting for FlowFile ot become available on queue");
                return;
            } catch (final Exception e) {
                logger.error("Failed to transfer FlowFiles to {}", client.getNodeIdentifier(), e);

                // Pause for 1 second before continuing on, so that we don't constantly
                // hit the other nodes and the network when we're in a bad state
                try {
                    Thread.sleep(1000L);
                } catch (final InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    protected void transferFlowFiles(final Set<FlowFileRecord> expiredRecords, final List<RepositoryRecord> abortedRecords, final long expirationMillis) throws IOException, InterruptedException {
        // Wait a small amount of time to pull a FlowFile from the queue so that we don't consume a lot of CPU
        // resources waiting for a FlowFile to become available.
        FlowFileRecord flowFile = queue.poll(expiredRecords, expirationMillis, maxPollMillis);
        if (flowFile == null) {
            return;
        }

        // If task has been stopped while waiting for a FlowFile, avoid sending it at all.
        if (isStopped()) {
            logger.debug("{} Transfer stopped so re-queuing {}", this, flowFile);
            queue.acknowledge(flowFile);
            queue.put(flowFile);
            return;
        }

        // Create a transaction, send FlowFiles, and complete the transaction.
        final LoadBalanceTransaction transaction;
        try {
            transaction = client.createTransaction(flowFileQueue.getIdentifier());
        } catch (final Exception e) {
            // If we are unable to even create a transaction, then we want to hand our entire
            // queue over to the failure destination. We must also ensure that we account for
            // the one FlowFile that we already have pulled from the queue.
            failureDestination.putAll(queue::packageForRebalance);
            queue.acknowledge(flowFile);
            failureDestination.putAll(Collections.singleton(flowFile));
            throw e;
        }

        final List<FlowFileRecord> flowFilesSent = new ArrayList<>();

        try {
            sendFlowFiles(flowFile, transaction, flowFilesSent, abortedRecords);

            transaction.complete();

            // We've now completed the transaction. We must now update the repositories and "keep the books", acknowledging the FlowFiles
            // with the queue so that its size remains accurate.
            updateRepositories(flowFilesSent, abortedRecords);
            queue.acknowledge(flowFilesSent);
            flowFileQueue.onTransfer(flowFilesSent);
        } catch (final Exception e) {
            handleFailure(flowFilesSent);
            transaction.abort();

            if (!abortedRecords.isEmpty()) {
                updateRepositories(Collections.emptyList(), abortedRecords);
            }

            throw e;
        } finally {
            final List<FlowFileRecord> abortedFlowFiles = abortedRecords.stream()
                .map(RepositoryRecord::getCurrent)
                .collect(Collectors.toList());

            queue.acknowledge(abortedFlowFiles);
            flowFileQueue.onAbort(abortedFlowFiles);
        }
    }

    private void handleFailure(final List<FlowFileRecord> flowFiles) {
        // Acknowledge FlowFile and re-queue it.
        queue.acknowledge(flowFiles);
        failureDestination.putAll(flowFiles);
    }

    private void sendFlowFiles(final FlowFileRecord initialFlowFile, final LoadBalanceTransaction transaction,
                               final List<FlowFileRecord> flowFilesSent, final List<RepositoryRecord> abortedRecords) throws IOException {

        final Set<FlowFileRecord> expiredRecords = new HashSet<>();
        final long expirationMillis = flowFileQueue.getFlowFileExpiration(TimeUnit.MILLISECONDS);
        final TransactionThreshold threshold = createThreshold();

        flowFilesSent.add(initialFlowFile);
        FlowFileRecord flowFile = initialFlowFile;

        while (true) {
            try (final InputStream flowFileContent = flowFileAccess.read(flowFile)) {
                // TODO: Allow for interrupting the transfer if stopped.
                transaction.send(flowFile, flowFileContent);
                threshold.adjust(1, flowFile.getSize());
            } catch (final ContentNotFoundException cnfe) {
                final StandardRepositoryRecord abortedRecord = handleContentNotFound(flowFile);

                // FlowFile not sent so remove from the 'flowFiles' list and add the corresponding RepositoryRecord to abortedRecords
                abortedRecords.add(abortedRecord);
                flowFilesSent.remove(flowFile);

                // If we hit too many aborted records, let's complete the transaction.
                if (abortedRecords.size() >= 10000) {
                    break;
                }
            }

            if (isStopped() || threshold.isThresholdMet()) {
                break;
            }

            flowFile = queue.poll(expiredRecords, expirationMillis);
            if (flowFile == null) {
                break;
            }

            flowFilesSent.add(flowFile);
        }
    }

    /**
     * Updates the FlowFileRepository, Provenance Repository, and claimant counts in the Content Repository.
     *
     * @param flowFilesSent the FlowFiles that were sent to another node.
     * @param abortedRecords the Repository Records for any FlowFile whose content was missing.
     */
    private void updateRepositories(final List<FlowFileRecord> flowFilesSent, final List<RepositoryRecord> abortedRecords) {
        // We update the Provenance Repository first. This way, even if we restart before we update the FlowFile repo, we have the record
        // that the data was sent in the Provenance Repository. We then update the content claims and finally the FlowFile Repository. We do it
        // in this order so that when the FlowFile repo is sync'ed to disk, we know which Content Claims are no longer in use. Updating the FlowFile
        // Repo first could result in holding those Content Claims on disk longer than we need to.
        //
        // Additionally, we are iterating over the FlowFiles sent multiple times. We could refactor this to iterate over them just once and then
        // create the Provenance Events and Repository Records in a single pass. Doing so, however, would mean that we need to keep both collections
        // of objects in heap at the same time. Using multiple passes allows the Provenance Events to be freed from heap by the GC before the Repo Records
        // are ever created.
        final List<ProvenanceEventRecord> provenanceEvents = new ArrayList<>(flowFilesSent.size() * 2 + abortedRecords.size());
        for (final FlowFileRecord sent : flowFilesSent) {
            // TODO: Should discuss with others -- do we want SEND/DROP event for each FlowFile here? Or a "TRANSFER" event? Or nothing because
            // it is staying within the comfy confines of a NiFi cluster??
            provenanceEvents.add(createSendEvent(sent));
            provenanceEvents.add(createDropEvent(sent));
        }

        for (final RepositoryRecord abortedRecord : abortedRecords) {
            final FlowFileRecord abortedFlowFile = abortedRecord.getCurrent();
            provenanceEvents.add(createDropEvent(abortedFlowFile, "Content Not Found"));
        }

        provRepo.registerEvents(provenanceEvents);

        // Update the FlowFile Repository & content claim counts last
        final List<RepositoryRecord> flowFileRepoRecords = flowFilesSent.stream()
            .map(this::createRepositoryRecord)
            .collect(Collectors.toCollection(ArrayList::new));

        flowFileRepoRecords.addAll(abortedRecords);

        // Decrement claimant count for each FlowFile.
        flowFileRepoRecords.stream()
            .map(RepositoryRecord::getCurrentClaim)
            .forEach(contentRepo::decrementClaimantCount);

        try {
            flowFileRepo.updateRepository(flowFileRepoRecords);
        } catch (final Exception e) {
            logger.error("Unable to update FlowFile repository to indicate that {} FlowFiles have been transferred to {}. "
                + "It is possible that these FlowFiles will be duplicated upon restart of NiFi.", flowFilesSent.size(), client.getNodeIdentifier(), e);
        }
    }

    private RepositoryRecord createRepositoryRecord(final FlowFileRecord flowFile) {
        final StandardRepositoryRecord record = new StandardRepositoryRecord(flowFileQueue, flowFile);
        record.markForDelete();
        return record;
    }

    private ProvenanceEventRecord createSendEvent(final FlowFileRecord flowFile) {

        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder()
            .fromFlowFile(flowFile)
            .setEventType(ProvenanceEventType.SEND)
            .setDetails("Re-distributed for Load-balanced connection")
            .setComponentId(flowFileQueue.getIdentifier())
            .setComponentType("Connection")
            .setSourceQueueIdentifier(flowFileQueue.getIdentifier())
            .setSourceSystemFlowFileIdentifier(flowFile.getAttribute(CoreAttributes.UUID.key()))
            .setTransitUri("nifi:connection:" + flowFileQueue.getIdentifier());

        final ContentClaim contentClaim = flowFile.getContentClaim();
        if (contentClaim != null) {
            final ResourceClaim resourceClaim = contentClaim.getResourceClaim();
            builder.setCurrentContentClaim(resourceClaim.getContainer(),resourceClaim.getSection() ,resourceClaim.getId(),
                    contentClaim.getOffset() + flowFile.getContentClaimOffset(), flowFile.getSize());

            builder.setPreviousContentClaim(resourceClaim.getContainer(),resourceClaim.getSection() ,resourceClaim.getId(),
                    contentClaim.getOffset() + flowFile.getContentClaimOffset(), flowFile.getSize());
        }

        final ProvenanceEventRecord sendEvent = builder.build();

        return sendEvent;
    }

    private ProvenanceEventRecord createDropEvent(final FlowFileRecord flowFile) {
        return createDropEvent(flowFile, null);
    }

    private ProvenanceEventRecord createDropEvent(final FlowFileRecord flowFile, final String details) {
        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder()
            .fromFlowFile(flowFile)
            .setEventType(ProvenanceEventType.DROP)
            .setDetails(details)
            .setComponentId(flowFileQueue.getIdentifier())
            .setComponentType("Connection")
            .setSourceQueueIdentifier(flowFileQueue.getIdentifier());

        final ContentClaim contentClaim = flowFile.getContentClaim();
        if (contentClaim != null) {
            final ResourceClaim resourceClaim = contentClaim.getResourceClaim();
            builder.setCurrentContentClaim(resourceClaim.getContainer(),resourceClaim.getSection() ,resourceClaim.getId(),
                    contentClaim.getOffset() + flowFile.getContentClaimOffset(), flowFile.getSize());

            builder.setPreviousContentClaim(resourceClaim.getContainer(),resourceClaim.getSection() ,resourceClaim.getId(),
                    contentClaim.getOffset() + flowFile.getContentClaimOffset(), flowFile.getSize());
        }

        final ProvenanceEventRecord dropEvent = builder.build();

        return dropEvent;
    }

    protected TransactionThreshold createThreshold() {
        return new SimpleLimitThreshold(1000, 10_000_000);
    }

    private StandardRepositoryRecord handleContentNotFound(final FlowFileRecord flowFile) {
        final ProvenanceEventRecord dropEvent = new StandardProvenanceEventRecord.Builder()
            .fromFlowFile(flowFile)
            .setEventType(ProvenanceEventType.DROP)
            .setDetails("Content Not Found")
            .setComponentId(flowFileQueue.getIdentifier())
            .setComponentType("Connection")
            .setSourceQueueIdentifier(flowFileQueue.getIdentifier())
            .build();

        provRepo.registerEvent(dropEvent);

        logger.error("Failed to find content for {} so unable to send it to {}", flowFile, client.getNodeIdentifier());
        eventReporter.reportEvent(Severity.ERROR, "Content Not Found", "Failed to transfer FlowFile to " + client.getNodeIdentifier() + " because could not find the FlowFile's content");

        final StandardRepositoryRecord record = new StandardRepositoryRecord(flowFileQueue, flowFile);
        record.markForAbort();

        return record;
    }

    public void stop() {
        stopped = true;
        logger.info(this + " Stopped transferring FlowFiles");
    }

    public boolean isStopped() {
        return stopped;
    }

    @Override
    public String toString() {
        return "TransferFlowFiles[queue=" + flowFileQueue.getIdentifier() + ", node=" + client.getNodeIdentifier() + "]";
    }
}
