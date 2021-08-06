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
package org.apache.nifi.registry.db.migration;

import org.apache.nifi.registry.db.entity.BucketItemEntityType;
import org.flywaydb.core.Flyway;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test running the legacy Flyway migrations against an in-memory H2 and then using the LegacyDatabaseService to
 * retrieve data. Purposely not using Spring test annotations here to avoid interfering with the normal DB context/flyway.
 */
public class TestLegacyDatabaseService {

    private DataSource dataSource;
    private JdbcTemplate jdbcTemplate;
    private Flyway flyway;

    private BucketEntityV1 bucketEntityV1;
    private FlowEntityV1 flowEntityV1;
    private FlowSnapshotEntityV1 flowSnapshotEntityV1;

    @Before
    public void setup() {
        dataSource = DataSourceBuilder.create()
                .url("jdbc:h2:mem:legacydb")
                .driverClassName("org.h2.Driver")
                .build();

        jdbcTemplate = new JdbcTemplate(dataSource);

        flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations("db/migration/original")
                .load();

        flyway.migrate();

        bucketEntityV1 = new BucketEntityV1();
        bucketEntityV1.setId("1");
        bucketEntityV1.setName("Bucket1");
        bucketEntityV1.setDescription("This is bucket 1");
        bucketEntityV1.setCreated(new Date());

        jdbcTemplate.update("INSERT INTO bucket (ID, NAME, DESCRIPTION, CREATED) VALUES (?, ?, ?, ?)",
                bucketEntityV1.getId(),
                bucketEntityV1.getName(),
                bucketEntityV1.getDescription(),
                bucketEntityV1.getCreated());

        flowEntityV1 = new FlowEntityV1();
        flowEntityV1.setId("1");
        flowEntityV1.setBucketId(bucketEntityV1.getId());
        flowEntityV1.setName("Flow1");
        flowEntityV1.setDescription("This is flow1");
        flowEntityV1.setCreated(new Date());
        flowEntityV1.setModified(new Date());

        jdbcTemplate.update("INSERT INTO bucket_item (ID, NAME, DESCRIPTION, CREATED, MODIFIED, ITEM_TYPE, BUCKET_ID) VALUES (?, ?, ?, ?, ?, ?, ?)",
                flowEntityV1.getId(),
                flowEntityV1.getName(),
                flowEntityV1.getDescription(),
                flowEntityV1.getCreated(),
                flowEntityV1.getModified(),
                BucketItemEntityType.FLOW.toString(),
                flowEntityV1.getBucketId());

        jdbcTemplate.update("INSERT INTO flow (ID) VALUES (?)", flowEntityV1.getId());

        flowSnapshotEntityV1 = new FlowSnapshotEntityV1();
        flowSnapshotEntityV1.setFlowId(flowEntityV1.getId());
        flowSnapshotEntityV1.setVersion(1);
        flowSnapshotEntityV1.setComments("This is v1");
        flowSnapshotEntityV1.setCreated(new Date());
        flowSnapshotEntityV1.setCreatedBy("user1");

        jdbcTemplate.update("INSERT INTO flow_snapshot (FLOW_ID, VERSION, CREATED, CREATED_BY, COMMENTS) VALUES (?, ?, ?, ?, ?)",
                flowSnapshotEntityV1.getFlowId(),
                flowSnapshotEntityV1.getVersion(),
                flowSnapshotEntityV1.getCreated(),
                flowSnapshotEntityV1.getCreatedBy(),
                flowSnapshotEntityV1.getComments());
    }

    @Test
    public void testGetLegacyData() {
        final LegacyDatabaseService service = new LegacyDatabaseService(dataSource);

        final List<BucketEntityV1> buckets = service.getAllBuckets();
        assertEquals(1, buckets.size());

        final BucketEntityV1 b = buckets.stream().findFirst().get();
        assertEquals(bucketEntityV1.getId(), b.getId());
        assertEquals(bucketEntityV1.getName(), b.getName());
        assertEquals(bucketEntityV1.getDescription(), b.getDescription());
        assertEquals(bucketEntityV1.getCreated(), b.getCreated());

        final List<FlowEntityV1> flows = service.getAllFlows();
        assertEquals(1, flows.size());

        final FlowEntityV1 f = flows.stream().findFirst().get();
        assertEquals(flowEntityV1.getId(), f.getId());
        assertEquals(flowEntityV1.getName(), f.getName());
        assertEquals(flowEntityV1.getDescription(), f.getDescription());
        assertEquals(flowEntityV1.getCreated(), f.getCreated());
        assertEquals(flowEntityV1.getModified(), f.getModified());
        assertEquals(flowEntityV1.getBucketId(), f.getBucketId());

        final List<FlowSnapshotEntityV1> flowSnapshots = service.getAllFlowSnapshots();
        assertEquals(1, flowSnapshots.size());

        final FlowSnapshotEntityV1 fs = flowSnapshots.stream().findFirst().get();
        assertEquals(flowSnapshotEntityV1.getFlowId(), fs.getFlowId());
        assertEquals(flowSnapshotEntityV1.getVersion(), fs.getVersion());
        assertEquals(flowSnapshotEntityV1.getComments(), fs.getComments());
        assertEquals(flowSnapshotEntityV1.getCreatedBy(), fs.getCreatedBy());
        assertEquals(flowSnapshotEntityV1.getCreated(), fs.getCreated());
    }

}
