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
package org.apache.nifi.processors.hive;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.messaging.AddPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.DropPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.messaging.InsertMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.nifi.hive.metastore.ThriftMetastore;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.TRANSACTIONAL_EVENT_LISTENERS;
import static org.apache.nifi.processors.hive.TriggerHiveMetaStoreEvent.METASTORE_NOTIFICATION_EVENT;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.condition.OS.WINDOWS;

public class TestTriggerHiveMetaStoreEvent {

    private TestRunner runner;
    private TriggerHiveMetaStoreEvent processor;
    private HiveMetaStoreClient metaStoreClient;

    private static final String TEST_DATABASE_NAME = "test_metastore";
    private static final String TEST_TABLE_NAME = "test_table";

    private static final List<FieldSchema> PARTITION_COLUMNS = Arrays.asList(
            new FieldSchema("year", "string", "year partition column"),
            new FieldSchema("month", "string", "month partition column")
    );

    @RegisterExtension
    public ThriftMetastore metastore = new ThriftMetastore()
            .withConfigOverrides(Collections.singletonMap(TRANSACTIONAL_EVENT_LISTENERS.getVarname(), "org.apache.hive.hcatalog.listener.DbNotificationListener"));

    @BeforeEach
    public void setUp() {
        processor = new TriggerHiveMetaStoreEvent();
        metaStoreClient = metastore.getMetaStoreClient();
    }

    private void initUnPartitionedTable() throws Exception {
        createTable(TEST_DATABASE_NAME, TEST_TABLE_NAME, Collections.emptyList(), metastore.getWarehouseLocation() + "/" + TEST_TABLE_NAME);
    }

    private void initPartitionedTable() throws Exception {
        Table table = createTable(TEST_DATABASE_NAME, TEST_TABLE_NAME, PARTITION_COLUMNS, metastore.getWarehouseLocation() + "/" + TEST_TABLE_NAME);
        createPartition(table, Lists.newArrayList("2017", "march"));
        createPartition(table, Lists.newArrayList("2017", "april"));
        createPartition(table, Lists.newArrayList("2018", "march"));
    }

    @DisabledOnOs(WINDOWS)
    @Test
    public void testInsertOnUnPartitionedTable() throws Exception {
        initUnPartitionedTable();

        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(TriggerHiveMetaStoreEvent.HIVE_CONFIGURATION_RESOURCES, metastore.getConfigurationLocation());
        runner.setProperty(TriggerHiveMetaStoreEvent.EVENT_TYPE, "put");
        runner.setProperty(TriggerHiveMetaStoreEvent.PATH, metastore.getWarehouseLocation() + "/" + TEST_TABLE_NAME + "/test_file");
        runner.setProperty(TriggerHiveMetaStoreEvent.DATABASE_NAME, TEST_DATABASE_NAME);
        runner.setProperty(TriggerHiveMetaStoreEvent.TABLE_NAME, TEST_TABLE_NAME);

        runner.setValidateExpressionUsage(false);
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(TriggerHiveMetaStoreEvent.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(TriggerHiveMetaStoreEvent.REL_SUCCESS).get(0);

        Assertions.assertEquals(EventMessage.EventType.INSERT.toString(), flowFile.getAttribute(METASTORE_NOTIFICATION_EVENT));

        CurrentNotificationEventId eventId = metaStoreClient.getCurrentNotificationEventId();
        NotificationEventResponse response = metaStoreClient.getNextNotification(eventId.getEventId() - 1, 0, null);
        NotificationEvent event = response.getEvents().get(0);

        InsertMessage insertMessage = MessageFactory.getInstance().getDeserializer().getInsertMessage(event.getMessage());
        insertMessage.getFiles().forEach(s -> assertEquals(s, metastore.getWarehouseLocation() + "/" + TEST_TABLE_NAME + "/test_file###"));

        assertEquals(event.getEventType(), EventMessage.EventType.INSERT.toString());
        assertEquals(event.getDbName(), TEST_DATABASE_NAME);
        assertEquals(insertMessage.getDB(), TEST_DATABASE_NAME);
        assertEquals(event.getTableName(), TEST_TABLE_NAME);
        assertEquals(insertMessage.getTable(), TEST_TABLE_NAME);
    }

    @DisabledOnOs(WINDOWS)
    @Test
    public void testInsertOnPartitionedTable() throws Exception {
        initPartitionedTable();
        assertEquals(PARTITION_COLUMNS, metaStoreClient.getTable(TEST_DATABASE_NAME, TEST_TABLE_NAME).getPartitionKeys());

        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(TriggerHiveMetaStoreEvent.HIVE_CONFIGURATION_RESOURCES, metastore.getConfigurationLocation());
        runner.setProperty(TriggerHiveMetaStoreEvent.EVENT_TYPE, "put");
        runner.setProperty(TriggerHiveMetaStoreEvent.PATH, metastore.getWarehouseLocation() + "/" + TEST_TABLE_NAME + "/year=2017/month=march/test_file");
        runner.setProperty(TriggerHiveMetaStoreEvent.DATABASE_NAME, TEST_DATABASE_NAME);
        runner.setProperty(TriggerHiveMetaStoreEvent.TABLE_NAME, TEST_TABLE_NAME);

        runner.setValidateExpressionUsage(false);
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(TriggerHiveMetaStoreEvent.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(TriggerHiveMetaStoreEvent.REL_SUCCESS).get(0);

        Assertions.assertEquals(EventMessage.EventType.INSERT.toString(), flowFile.getAttribute(METASTORE_NOTIFICATION_EVENT));

        CurrentNotificationEventId eventId = metaStoreClient.getCurrentNotificationEventId();
        NotificationEventResponse response = metaStoreClient.getNextNotification(eventId.getEventId() - 1, 0, null);
        NotificationEvent event = response.getEvents().get(0);

        InsertMessage insertMessage = MessageFactory.getInstance().getDeserializer().getInsertMessage(event.getMessage());
        insertMessage.getFiles().forEach(s -> assertEquals(s, metastore.getWarehouseLocation() + "/" + TEST_TABLE_NAME + "/year=2017/month=march/test_file###"));

        assertEquals(event.getEventType(), EventMessage.EventType.INSERT.toString());
        assertEquals(event.getDbName(), TEST_DATABASE_NAME);
        assertEquals(insertMessage.getDB(), TEST_DATABASE_NAME);
        assertEquals(event.getTableName(), TEST_TABLE_NAME);
        assertEquals(insertMessage.getTable(), TEST_TABLE_NAME);
    }

    @DisabledOnOs(WINDOWS)
    @Test
    public void testAddPartition() throws Exception {
        initPartitionedTable();
        assertEquals(PARTITION_COLUMNS, metaStoreClient.getTable(TEST_DATABASE_NAME, TEST_TABLE_NAME).getPartitionKeys());

        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(TriggerHiveMetaStoreEvent.HIVE_CONFIGURATION_RESOURCES, metastore.getConfigurationLocation());
        runner.setProperty(TriggerHiveMetaStoreEvent.EVENT_TYPE, "put");
        runner.setProperty(TriggerHiveMetaStoreEvent.PATH, metastore.getWarehouseLocation() + "/" + TEST_TABLE_NAME + "/year=2017/month=june/test_file");
        runner.setProperty(TriggerHiveMetaStoreEvent.DATABASE_NAME, TEST_DATABASE_NAME);
        runner.setProperty(TriggerHiveMetaStoreEvent.TABLE_NAME, TEST_TABLE_NAME);

        runner.setValidateExpressionUsage(false);
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(TriggerHiveMetaStoreEvent.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(TriggerHiveMetaStoreEvent.REL_SUCCESS).get(0);

        Assertions.assertEquals(EventMessage.EventType.ADD_PARTITION.toString(), flowFile.getAttribute(METASTORE_NOTIFICATION_EVENT));

        CurrentNotificationEventId eventId = metaStoreClient.getCurrentNotificationEventId();
        NotificationEventResponse response = metaStoreClient.getNextNotification(eventId.getEventId() - 1, 0, null);
        NotificationEvent event = response.getEvents().get(0);

        AddPartitionMessage addPartitionMessage = MessageFactory.getInstance().getDeserializer().getAddPartitionMessage(event.getMessage());
        Map<String, String> expectedPartition = new HashMap<String, String>() {{
            put("year", "2017");
            put("month", "june");
        }};

        assertEquals(addPartitionMessage.getPartitions().get(0), expectedPartition);
        assertEquals(event.getEventType(), EventMessage.EventType.ADD_PARTITION.toString());
        assertEquals(event.getDbName(), TEST_DATABASE_NAME);
        assertEquals(addPartitionMessage.getDB(), TEST_DATABASE_NAME);
        assertEquals(event.getTableName(), TEST_TABLE_NAME);
        assertEquals(addPartitionMessage.getTable(), TEST_TABLE_NAME);

        assertDoesNotThrow(() -> metaStoreClient.getPartition(TEST_DATABASE_NAME, TEST_TABLE_NAME, Arrays.asList("2017", "june")));
    }

    @DisabledOnOs(WINDOWS)
    @Test
    public void testDropPartition() throws Exception {
        initPartitionedTable();
        assertEquals(PARTITION_COLUMNS, metaStoreClient.getTable(TEST_DATABASE_NAME, TEST_TABLE_NAME).getPartitionKeys());

        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(TriggerHiveMetaStoreEvent.HIVE_CONFIGURATION_RESOURCES, metastore.getConfigurationLocation());
        runner.setProperty(TriggerHiveMetaStoreEvent.EVENT_TYPE, "delete");
        runner.setProperty(TriggerHiveMetaStoreEvent.PATH, metastore.getWarehouseLocation() + "/" + TEST_TABLE_NAME + "/year=2017/month=march/test_file");
        runner.setProperty(TriggerHiveMetaStoreEvent.DATABASE_NAME, TEST_DATABASE_NAME);
        runner.setProperty(TriggerHiveMetaStoreEvent.TABLE_NAME, TEST_TABLE_NAME);

        runner.setValidateExpressionUsage(false);
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(TriggerHiveMetaStoreEvent.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(TriggerHiveMetaStoreEvent.REL_SUCCESS).get(0);

        Assertions.assertEquals(EventMessage.EventType.DROP_PARTITION.toString(), flowFile.getAttribute(METASTORE_NOTIFICATION_EVENT));

        CurrentNotificationEventId eventId = metaStoreClient.getCurrentNotificationEventId();
        NotificationEventResponse response = metaStoreClient.getNextNotification(eventId.getEventId() - 1, 0, null);
        NotificationEvent event = response.getEvents().get(0);

        DropPartitionMessage dropPartitionMessage = MessageFactory.getInstance().getDeserializer().getDropPartitionMessage(event.getMessage());
        Map<String, String> expectedPartition = new HashMap<String, String>() {{
            put("year", "2017");
            put("month", "march");
        }};

        assertEquals(dropPartitionMessage.getPartitions().get(0), expectedPartition);
        assertEquals(event.getEventType(), EventMessage.EventType.DROP_PARTITION.toString());
        assertEquals(event.getDbName(), TEST_DATABASE_NAME);
        assertEquals(dropPartitionMessage.getDB(), TEST_DATABASE_NAME);
        assertEquals(event.getTableName(), TEST_TABLE_NAME);
        assertEquals(dropPartitionMessage.getTable(), TEST_TABLE_NAME);

        assertThrows(NoSuchObjectException.class, () -> metaStoreClient.getPartition(TEST_DATABASE_NAME, TEST_TABLE_NAME, Arrays.asList("2017", "june")));
    }

    @DisabledOnOs(WINDOWS)
    @Test
    public void testUnknownEventType() throws Exception {
        initUnPartitionedTable();

        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(TriggerHiveMetaStoreEvent.HIVE_CONFIGURATION_RESOURCES, metastore.getConfigurationLocation());
        runner.setProperty(TriggerHiveMetaStoreEvent.EVENT_TYPE, "unknown");
        runner.setProperty(TriggerHiveMetaStoreEvent.PATH, metastore.getWarehouseLocation() + "/" + TEST_TABLE_NAME + "/test_file");
        runner.setProperty(TriggerHiveMetaStoreEvent.DATABASE_NAME, TEST_DATABASE_NAME);
        runner.setProperty(TriggerHiveMetaStoreEvent.TABLE_NAME, TEST_TABLE_NAME);

        runner.setValidateExpressionUsage(false);
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(TriggerHiveMetaStoreEvent.REL_FAILURE, 1);
    }

    private Table createTable(String databaseName, String tableName, List<FieldSchema> partitionColumns, String location) throws Exception {
        return new TableBuilder()
                .setDbName(databaseName)
                .setTableName(tableName)
                .addCol("id", "int", "id column")
                .addCol("value", "string", "value column")
                .setPartCols(partitionColumns)
                .setType(TableType.EXTERNAL_TABLE.name())
                .setLocation(location)
                .create(metaStoreClient, metastore.getConfiguration());
    }

    private void createPartition(Table table, List<String> values) throws Exception {
        new PartitionBuilder()
                .inTable(table)
                .setValues(values)
                .addToTable(metaStoreClient, metastore.getConfiguration());
    }
}
