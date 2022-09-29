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
package org.apache.nifi.processors.azure.eventhub.position;

import com.azure.core.util.BinaryData;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Event Position Provider using Azure Blob Storage implemented in Azure Event Hubs SDK Version 3
 */
public class LegacyBlobStorageEventPositionProvider implements EventPositionProvider {
    private static final String LEASE_SEQUENCE_NUMBER_FIELD = "sequenceNumber";

    private static final Logger logger = LoggerFactory.getLogger(LegacyBlobStorageEventPositionProvider.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final BlobContainerAsyncClient blobContainerAsyncClient;

    private final String consumerGroup;

    public LegacyBlobStorageEventPositionProvider(
            final BlobContainerAsyncClient blobContainerAsyncClient,
            final String consumerGroup
    ) {
        this.blobContainerAsyncClient = Objects.requireNonNull(blobContainerAsyncClient, "Client required");
        this.consumerGroup = Objects.requireNonNull(consumerGroup, "Consumer Group required");
    }

    /**
     * Get Initial Partition Event Position using Azure Blob Storage as persisted in
     * com.microsoft.azure.eventprocessorhost.AzureStorageCheckpointLeaseManager
     *
     * @return Map of Partition and Event Position or empty when no checkpoints found
     */
    @Override
    public Map<String, EventPosition> getInitialPartitionEventPosition() {
        final Map<String, EventPosition> partitionEventPosition;

        if (containerExists()) {
            final BlobListDetails blobListDetails = new BlobListDetails().setRetrieveMetadata(true);
            final ListBlobsOptions listBlobsOptions = new ListBlobsOptions().setPrefix(consumerGroup).setDetails(blobListDetails);
            final Iterable<BlobItem> blobItems = blobContainerAsyncClient.listBlobs(listBlobsOptions).toIterable();
            partitionEventPosition = getPartitionEventPosition(blobItems);
        } else {
            partitionEventPosition = Collections.emptyMap();
        }

        return partitionEventPosition;
    }

    private Map<String, EventPosition> getPartitionEventPosition(final Iterable<BlobItem> blobItems) {
        final Map<String, EventPosition> partitionEventPosition = new LinkedHashMap<>();

        for (final BlobItem blobItem : blobItems) {
            if (Boolean.TRUE.equals(blobItem.isPrefix())) {
                continue;
            }

            final String partitionId = getPartitionId(blobItem);
            final EventPosition eventPosition = getEventPosition(blobItem);
            if (eventPosition == null) {
                logger.info("Legacy Event Position not found for Partition [{}] Blob [{}]", partitionId, blobItem.getName());
            } else {
                partitionEventPosition.put(partitionId, eventPosition);
            }
        }

        return partitionEventPosition;
    }

    private String getPartitionId(final BlobItem blobItem) {
        final String blobItemName = blobItem.getName();
        final Path blobItemPath = Paths.get(blobItemName);
        final Path blobItemFileName = blobItemPath.getFileName();
        return blobItemFileName.toString();
    }

    private EventPosition getEventPosition(final BlobItem blobItem) {
        final EventPosition eventPosition;

        final String blobName = blobItem.getName();
        final BlobAsyncClient blobAsyncClient = blobContainerAsyncClient.getBlobAsyncClient(blobName);

        if (itemExists(blobAsyncClient)) {
            final BinaryData content = blobAsyncClient.downloadContent().block();
            if (content == null) {
                throw new IllegalStateException(String.format("Legacy Event Position content not found [%s]", blobName));
            }

            try {
                // Read com.microsoft.azure.eventprocessorhost.AzureBlobLease from JSON
                final JsonNode lease = objectMapper.readTree(content.toBytes());
                if (lease.hasNonNull(LEASE_SEQUENCE_NUMBER_FIELD)) {
                    final JsonNode sequenceNumberField = lease.get(LEASE_SEQUENCE_NUMBER_FIELD);
                    final long sequenceNumber = sequenceNumberField.asLong();
                    eventPosition = EventPosition.fromSequenceNumber(sequenceNumber);
                } else {
                    eventPosition = null;
                }
            } catch (final IOException e) {
                throw new UncheckedIOException(String.format("Reading Legacy Event Position Failed [%s]", blobName), e);
            }
        } else {
            logger.info("Legacy Event Position not found [{}]", blobName);
            eventPosition = null;
        }

        return eventPosition;
    }

    private boolean containerExists() {
        return Boolean.TRUE.equals(blobContainerAsyncClient.exists().block());
    }

    private boolean itemExists(final BlobAsyncClient blobAsyncClient) {
        return Boolean.TRUE.equals(blobAsyncClient.exists().block());
    }
}
