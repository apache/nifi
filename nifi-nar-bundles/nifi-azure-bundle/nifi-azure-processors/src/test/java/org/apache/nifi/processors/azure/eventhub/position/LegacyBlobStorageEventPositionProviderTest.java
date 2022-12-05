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

import com.azure.core.http.rest.PagedFlux;
import com.azure.core.http.rest.PagedResponse;
import com.azure.core.util.BinaryData;
import com.azure.core.util.IterableStream;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LegacyBlobStorageEventPositionProviderTest {
    private static final String CONSUMER_GROUP = "$Default";

    private static final String PARTITION_ID = "1";

    private static final String BLOB_NAME = String.format("/Partitions/%s", PARTITION_ID);

    private static final String EMPTY_OBJECT = "{}";

    private static final long SEQUENCE_NUMBER = 10;

    private static final String SEQUENCE_NUMBER_OBJECT = String.format("{\"sequenceNumber\":%d}", SEQUENCE_NUMBER);

    @Mock
    BlobContainerAsyncClient blobContainerAsyncClient;

    @Mock
    BlobAsyncClient blobAsyncClient;

    @Mock
    PagedResponse<BlobItem> pagedResponse;

    LegacyBlobStorageEventPositionProvider provider;

    @BeforeEach
    void setProvider() {
        provider = new LegacyBlobStorageEventPositionProvider(blobContainerAsyncClient, CONSUMER_GROUP);
    }

    @Test
    void testContainerNotFound() {
        when(blobContainerAsyncClient.exists()).thenReturn(Mono.just(Boolean.FALSE));

        final Map<String, EventPosition> partitionEventPosition = provider.getInitialPartitionEventPosition();

        assertNotNull(partitionEventPosition);
        assertTrue(partitionEventPosition.isEmpty());
    }

    @Test
    void testContainerEmpty() {
        when(blobContainerAsyncClient.exists()).thenReturn(Mono.just(Boolean.TRUE));

        final PagedFlux<BlobItem> blobItems = new PagedFlux<>(() -> Mono.just(pagedResponse));
        when(blobContainerAsyncClient.listBlobs(any(ListBlobsOptions.class))).thenReturn(blobItems);

        final Map<String, EventPosition> partitionEventPosition = provider.getInitialPartitionEventPosition();

        assertNotNull(partitionEventPosition);
        assertTrue(partitionEventPosition.isEmpty());
    }

    @Test
    void testSequenceNumberNotFound() {
        setBlobData(EMPTY_OBJECT);

        final Map<String, EventPosition> partitionEventPosition = provider.getInitialPartitionEventPosition();

        assertNotNull(partitionEventPosition);
        assertTrue(partitionEventPosition.isEmpty());
    }

    @Test
    void testSequenceNumberFound() {
        setBlobData(SEQUENCE_NUMBER_OBJECT);

        final Map<String, EventPosition> partitionEventPosition = provider.getInitialPartitionEventPosition();

        assertNotNull(partitionEventPosition);
        assertFalse(partitionEventPosition.isEmpty());

        final EventPosition eventPosition = partitionEventPosition.get(PARTITION_ID);
        assertNotNull(eventPosition);

        assertEquals(SEQUENCE_NUMBER, eventPosition.getSequenceNumber());
    }

    private void setBlobData(final String blobData) {
        when(blobContainerAsyncClient.exists()).thenReturn(Mono.just(Boolean.TRUE));

        final BlobItem directoryBlobItem = new BlobItem();
        directoryBlobItem.setIsPrefix(true);

        final BlobItem blobItem = new BlobItem();
        blobItem.setIsPrefix(false);
        blobItem.setName(BLOB_NAME);

        final IterableStream<BlobItem> blobItems = IterableStream.of(Arrays.asList(directoryBlobItem, blobItem));
        when(pagedResponse.getElements()).thenReturn(blobItems);
        final PagedFlux<BlobItem> pagedItems = new PagedFlux<>(() -> Mono.just(pagedResponse));
        when(blobContainerAsyncClient.listBlobs(any(ListBlobsOptions.class))).thenReturn(pagedItems);

        when(blobContainerAsyncClient.getBlobAsyncClient(eq(BLOB_NAME))).thenReturn(blobAsyncClient);

        when(blobAsyncClient.exists()).thenReturn(Mono.just(Boolean.TRUE));
        final BinaryData objectData = BinaryData.fromString(blobData);
        when(blobAsyncClient.downloadContent()).thenReturn(Mono.just(objectData));
    }
}
