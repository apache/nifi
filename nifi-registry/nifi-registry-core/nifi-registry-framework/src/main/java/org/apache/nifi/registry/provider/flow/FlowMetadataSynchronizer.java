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
package org.apache.nifi.registry.provider.flow;

import org.apache.nifi.registry.db.entity.BucketEntity;
import org.apache.nifi.registry.db.entity.BucketItemEntityType;
import org.apache.nifi.registry.db.entity.FlowEntity;
import org.apache.nifi.registry.db.entity.FlowSnapshotEntity;
import org.apache.nifi.registry.flow.FlowPersistenceProvider;
import org.apache.nifi.registry.flow.MetadataAwareFlowPersistenceProvider;
import org.apache.nifi.registry.metadata.BucketMetadata;
import org.apache.nifi.registry.metadata.FlowMetadata;
import org.apache.nifi.registry.metadata.FlowSnapshotMetadata;
import org.apache.nifi.registry.service.MetadataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;

@Component
public class FlowMetadataSynchronizer {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlowMetadataSynchronizer.class);

    private MetadataService metadataService;
    private FlowPersistenceProvider persistenceProvider;

    @Autowired
    public FlowMetadataSynchronizer(final MetadataService metadataService,
                                    final FlowPersistenceProvider persistenceProvider) {
        this.metadataService = metadataService;
        this.persistenceProvider = persistenceProvider;
    }

    @EventListener(ContextRefreshedEvent.class)
    public void synchronize() {
        LOGGER.info("**************************************************");

        if (!(persistenceProvider instanceof MetadataAwareFlowPersistenceProvider)) {
            LOGGER.info("*  FlowPersistenceProvider is not metadata-aware, nothing to synchronize");
            LOGGER.info("**************************************************");
            return;
        } else {
            LOGGER.info("*  Found metadata-aware FlowPersistenceProvider...");
        }

        if (!metadataService.getAllBuckets().isEmpty()) {
            LOGGER.info("*  Found existing buckets, will not synchronize metadata");
            LOGGER.info("**************************************************");
            return;
        }

        final MetadataAwareFlowPersistenceProvider metadataAwareFlowPersistenceProvider = (MetadataAwareFlowPersistenceProvider) persistenceProvider;
        LOGGER.info("*  Synchronizing metadata from FlowPersistenceProvider to metadata database...");

        final List<BucketMetadata> metadata = metadataAwareFlowPersistenceProvider.getMetadata();
        LOGGER.info("*  Synchronizing {} bucket(s)", new Object[]{metadata.size()});

        for (final BucketMetadata bucketMetadata : metadata) {
            final BucketEntity bucketEntity = new BucketEntity();
            bucketEntity.setId(bucketMetadata.getIdentifier());
            bucketEntity.setName(bucketMetadata.getName());
            bucketEntity.setDescription(bucketMetadata.getDescription());
            bucketEntity.setCreated(new Date());
            metadataService.createBucket(bucketEntity);
            createFlows(bucketMetadata);
        }

        LOGGER.info("*  Done synchronizing metadata!");
        LOGGER.info("**************************************************");
    }

    private void createFlows(final BucketMetadata bucketMetadata) {
        LOGGER.info("*  Synchronizing {} flow(s) for bucket {}",
                new Object[]{bucketMetadata.getFlowMetadata().size(), bucketMetadata.getIdentifier()});

        for (final FlowMetadata flowMetadata : bucketMetadata.getFlowMetadata()) {
            final FlowEntity flowEntity = new FlowEntity();
            flowEntity.setType(BucketItemEntityType.FLOW);
            flowEntity.setId(flowMetadata.getIdentifier());
            flowEntity.setName(flowMetadata.getName());
            flowEntity.setDescription(flowMetadata.getDescription());
            flowEntity.setBucketId(bucketMetadata.getIdentifier());
            flowEntity.setCreated(new Date());
            flowEntity.setModified(new Date());
            metadataService.createFlow(flowEntity);

            createFlowSnapshots(flowMetadata);
        }
    }

    private void createFlowSnapshots(final FlowMetadata flowMetadata) {
        LOGGER.info("*  Synchronizing {} version(s) for flow {}",
                new Object[]{flowMetadata.getFlowSnapshotMetadata().size(),
                        flowMetadata.getIdentifier()});

        for (final FlowSnapshotMetadata snapshotMetadata : flowMetadata.getFlowSnapshotMetadata()) {
            final FlowSnapshotEntity snapshotEntity = new FlowSnapshotEntity();
            snapshotEntity.setFlowId(flowMetadata.getIdentifier());
            snapshotEntity.setVersion(snapshotMetadata.getVersion());
            snapshotEntity.setComments(snapshotMetadata.getComments());

            String author = snapshotMetadata.getAuthor();
            if (author == null) {
                author = "unknown";
            }
            snapshotEntity.setCreatedBy(author);

            Long created = snapshotMetadata.getCreated();
            if (created == null) {
                created = Long.valueOf(System.currentTimeMillis());
            }
            snapshotEntity.setCreated(new Date(created));

            metadataService.createFlowSnapshot(snapshotEntity);
        }
    }

}
