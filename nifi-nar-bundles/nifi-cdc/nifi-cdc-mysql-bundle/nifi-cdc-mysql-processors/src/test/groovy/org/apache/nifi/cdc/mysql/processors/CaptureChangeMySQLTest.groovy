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
package org.apache.nifi.cdc.mysql.processors

import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData
import com.github.shyiko.mysql.binlog.event.Event
import com.github.shyiko.mysql.binlog.event.EventData
import com.github.shyiko.mysql.binlog.event.EventHeaderV4
import com.github.shyiko.mysql.binlog.event.EventType
import com.github.shyiko.mysql.binlog.event.QueryEventData
import com.github.shyiko.mysql.binlog.event.RotateEventData
import com.github.shyiko.mysql.binlog.event.TableMapEventData
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData
import groovy.json.JsonSlurper
import org.apache.commons.io.output.WriterOutputStream
import org.apache.nifi.cdc.mysql.MockBinlogClient
import org.apache.nifi.cdc.mysql.event.BinlogEventInfo
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.components.state.Scope
import org.apache.nifi.controller.AbstractControllerService
import org.apache.nifi.distributed.cache.client.Deserializer
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService
import org.apache.nifi.distributed.cache.client.Serializer
import org.apache.nifi.flowfile.attributes.CoreAttributes
import org.apache.nifi.logging.ComponentLog

import org.apache.nifi.cdc.event.ColumnDefinition
import org.apache.nifi.cdc.event.TableInfo
import org.apache.nifi.cdc.event.TableInfoCacheKey
import org.apache.nifi.cdc.event.io.EventWriter
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.provenance.ProvenanceEventType
import org.apache.nifi.reporting.InitializationException
import org.apache.nifi.state.MockStateManager
import org.apache.nifi.util.MockComponentLog
import org.apache.nifi.util.MockControllerServiceInitializationContext
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.After
import org.junit.Before
import org.junit.Test

import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.util.concurrent.TimeoutException
import java.util.regex.Matcher
import java.util.regex.Pattern

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertTrue
import static org.mockito.Matchers.anyString
import static org.mockito.Mockito.mock
import static org.mockito.Mockito.when

/**
 * Unit test(s) for MySQL CDC
 */
class CaptureChangeMySQLTest {
    CaptureChangeMySQL processor
    TestRunner testRunner
    MockBinlogClient client

    @Before
    void setUp() throws Exception {
        processor = new MockCaptureChangeMySQL()
        testRunner = TestRunners.newTestRunner(processor)
        client = new MockBinlogClient('localhost', 3306, 'root', 'password')
    }

    @After
    void tearDown() throws Exception {

    }

    @Test
    void testConnectionFailures() throws Exception {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, 'file:///path/to/mysql-connector-java-5.1.38-bin.jar')
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, 'localhost:3306')
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, 'root')
        client.connectionError = true
        try {
            testRunner.run()
        } catch (AssertionError ae) {
            def pe = ae.getCause()
            assertTrue(pe instanceof ProcessException)
            def ioe = pe.getCause()
            assertTrue(ioe instanceof IOException)
            assertEquals('Could not connect binlog client to any of the specified hosts due to: Error during connect', ioe.getMessage())
            assertTrue(ioe.getCause() instanceof IOException)
        }
        client.connectionError = false

        client.connectionTimeout = true
        try {
            testRunner.run()
        } catch (AssertionError ae) {
            def pe = ae.getCause()
            assertTrue(pe instanceof ProcessException)
            def ioe = pe.getCause()
            assertTrue(ioe instanceof IOException)
            assertEquals('Could not connect binlog client to any of the specified hosts due to: Connection timed out', ioe.getMessage())
            assertTrue(ioe.getCause() instanceof TimeoutException)
        }
        client.connectionTimeout = false
    }

    @Test
    void testBeginCommitTransaction() throws Exception {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, 'file:///path/to/mysql-connector-java-5.1.38-bin.jar')
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, 'localhost:3306')
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, 'root')
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, '2 seconds')
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_BEGIN_COMMIT, 'true')

        testRunner.run(1, false, true)

        // ROTATE
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.ROTATE, nextPosition: 2] as EventHeaderV4,
                [binlogFilename: 'master.000001', binlogPosition: 4L] as RotateEventData
        ))

        // BEGIN
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 4] as EventHeaderV4,
                [database: 'myDB', sql: 'BEGIN'] as QueryEventData
        ))

        // COMMIT
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.XID, nextPosition: 12] as EventHeaderV4,
                {} as EventData
        ))

        testRunner.run(1, true, false)

        def resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS)
        assertEquals(2, resultFiles.size())
    }

    @Test
    void testBeginCommitTransactionFiltered() throws Exception {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, 'file:///path/to/mysql-connector-java-5.1.38-bin.jar')
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, 'localhost:3306')
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, 'root')
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, '2 seconds')
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_BEGIN_COMMIT, 'false')
        testRunner.setProperty(CaptureChangeMySQL.INIT_SEQUENCE_ID, '10')

        testRunner.run(1, false, true)

        // ROTATE
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.ROTATE, nextPosition: 2] as EventHeaderV4,
                [binlogFilename: 'master.000001', binlogPosition: 4L] as RotateEventData
        ))

        // BEGIN
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 4] as EventHeaderV4,
                [database: 'myDB', sql: 'BEGIN'] as QueryEventData
        ))

        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.TABLE_MAP, nextPosition: 6] as EventHeaderV4,
                [tableId: 1, database: 'myDB', table: 'myTable', columnTypes: [4, -4] as byte[]] as TableMapEventData
        ))

        def cols = new BitSet()
        cols.set(1)
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.EXT_WRITE_ROWS, nextPosition: 8] as EventHeaderV4,
                [tableId: 1, includedColumns: cols,
                 rows   : [[2, 'Smith'] as Serializable[]] as List<Serializable[]>] as WriteRowsEventData
        ))

        // COMMIT
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.XID, nextPosition: 12] as EventHeaderV4,
                {} as EventData
        ))

        testRunner.run(1, true, false)

        def resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS)
        assertEquals(1, resultFiles.size())
        assertEquals('10', resultFiles[0].getAttribute(EventWriter.SEQUENCE_ID_KEY))
    }

    @Test
    void testInitialSequenceIdIgnoredWhenStatePresent() throws Exception {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, 'file:///path/to/mysql-connector-java-5.1.38-bin.jar')
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, 'localhost:3306')
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, 'root')
        testRunner.setProperty(CaptureChangeMySQL.PASSWORD, 'password')
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, '2 seconds')
        testRunner.setProperty(CaptureChangeMySQL.INIT_SEQUENCE_ID, '10')
        testRunner.getStateManager().setState([("${EventWriter.SEQUENCE_ID_KEY}".toString()): '1'], Scope.CLUSTER)

        testRunner.run(1, false, true)

        // ROTATE
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.ROTATE, nextPosition: 2] as EventHeaderV4,
                [binlogFilename: 'master.000001', binlogPosition: 4L] as RotateEventData
        ))

        // BEGIN
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 4] as EventHeaderV4,
                [database: 'myDB', sql: 'BEGIN'] as QueryEventData
        ))

        // COMMIT
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.XID, nextPosition: 12] as EventHeaderV4,
                {} as EventData
        ))

        testRunner.run(1, true, false)

        def resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS)

        resultFiles.eachWithIndex {e, i ->
            // Sequence ID should start from 1 (as was put into the state map), showing that the
            // Initial Sequence ID value was ignored
            assertEquals(i + 1, Long.valueOf(e.getAttribute(EventWriter.SEQUENCE_ID_KEY)))
        }
    }

    @Test
    void testInitialSequenceIdNoStatePresent() throws Exception {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, 'file:///path/to/mysql-connector-java-5.1.38-bin.jar')
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, 'localhost:3306')
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, 'root')
        testRunner.setProperty(CaptureChangeMySQL.PASSWORD, 'password')
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, '2 seconds')
        testRunner.setProperty(CaptureChangeMySQL.INIT_SEQUENCE_ID, '10')

        testRunner.run(1, false, true)

        // ROTATE
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.ROTATE, nextPosition: 2] as EventHeaderV4,
                [binlogFilename: 'master.000001', binlogPosition: 4L] as RotateEventData
        ))

        // BEGIN
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 4] as EventHeaderV4,
                [database: 'myDB', sql: 'BEGIN'] as QueryEventData
        ))

        // COMMIT
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.XID, nextPosition: 12] as EventHeaderV4,
                {} as EventData
        ))

        testRunner.run(1, true, false)

        def resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS)

        resultFiles.eachWithIndex {e, i ->
            assertEquals(i + 10, Long.valueOf(e.getAttribute(EventWriter.SEQUENCE_ID_KEY)))
        }
    }

    @Test(expected = AssertionError.class)
    void testCommitWithoutBegin() throws Exception {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, 'file:///path/to/mysql-connector-java-5.1.38-bin.jar')
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, 'localhost:3306')
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, 'root')
        testRunner.setProperty(CaptureChangeMySQL.PASSWORD, 'password')
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, '2 seconds')

        testRunner.run(1, false, true)

        // COMMIT
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.XID, nextPosition: 12] as EventHeaderV4,
                {} as EventData
        ))

        testRunner.run(1, true, false)
    }

    @Test
    void testExtendedTransaction() throws Exception {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, 'file:///path/to/mysql-connector-java-5.1.38-bin.jar')
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, 'localhost:3306')
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, 'root')
        testRunner.setProperty(CaptureChangeMySQL.PASSWORD, 'password')
        testRunner.setProperty(CaptureChangeMySQL.SERVER_ID, '1')
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, '2 seconds')
        testRunner.setProperty(CaptureChangeMySQL.INIT_BINLOG_FILENAME, 'master.000001')
        testRunner.setProperty(CaptureChangeMySQL.INIT_BINLOG_POSITION, '4')
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_BEGIN_COMMIT, 'true')
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_DDL_EVENTS, 'true')
        final DistributedMapCacheClientImpl cacheClient = createCacheClient()
        def clientProperties = [:]
        clientProperties.put(DistributedMapCacheClientService.HOSTNAME.getName(), 'localhost')
        testRunner.addControllerService('client', cacheClient, clientProperties)
        testRunner.setProperty(CaptureChangeMySQL.DIST_CACHE_CLIENT, 'client')
        testRunner.enableControllerService(cacheClient)


        testRunner.run(1, false, true)

        // ROTATE scenario
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.ROTATE, nextPosition: 2] as EventHeaderV4,
                [binlogFilename: 'master.000001', binlogPosition: 4L] as RotateEventData
        ))

        // INSERT scenario
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 4] as EventHeaderV4,
                [database: 'myDB', sql: 'BEGIN'] as QueryEventData
        ))

        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.TABLE_MAP, nextPosition: 6] as EventHeaderV4,
                [tableId: 1, database: 'myDB', table: 'myTable', columnTypes: [4, -4] as byte[]] as TableMapEventData
        ))

        def cols = new BitSet()
        cols.set(1)
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.EXT_WRITE_ROWS, nextPosition: 8] as EventHeaderV4,
                [tableId: 1, includedColumns: cols,
                 rows   : [[2, 'Smith'] as Serializable[], [3, 'Jones'] as Serializable[], [10, 'Cruz'] as Serializable[]] as List<Serializable[]>] as WriteRowsEventData
        ))

        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.XID, nextPosition: 12] as EventHeaderV4,
                {} as EventData
        ))

        // UPDATE scenario
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 16] as EventHeaderV4,
                [database: 'myDB', sql: 'BEGIN'] as QueryEventData
        ))

        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.TABLE_MAP, nextPosition: 18] as EventHeaderV4,
                [tableId: 1, database: 'myDB', table: 'myTable', columnTypes: [4, -4] as byte[]] as TableMapEventData
        ))

        def colsBefore = new BitSet()
        colsBefore.set(0)
        colsBefore.set(1)
        def colsAfter = new BitSet()
        colsAfter.set(1)
        Map.Entry<Serializable[], Serializable[]> updateMapEntry = new Map.Entry<Serializable[], Serializable[]>() {
            Serializable[] getKey() {
                return [2, 'Smith'] as Serializable[]
            }

            @Override
            Serializable[] getValue() {
                return [3, 'Jones'] as Serializable[]
            }

            @Override
            Serializable[] setValue(Serializable[] value) {
                return [3, 'Jones'] as Serializable[]
            }
        }

        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.UPDATE_ROWS, nextPosition: 20] as EventHeaderV4,
                [tableId: 1, includedColumnsBeforeUpdate: colsBefore, includedColumns: colsAfter, rows: [updateMapEntry]
                        as List<Map.Entry<Serializable[], Serializable[]>>] as UpdateRowsEventData
        ))

        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.XID, nextPosition: 24] as EventHeaderV4,
                {} as EventData
        ))

        // ROTATE scenario
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.ROTATE, nextPosition: 26] as EventHeaderV4,
                [binlogFilename: 'master.000002', binlogPosition: 4L] as RotateEventData
        ))

        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 28] as EventHeaderV4,
                [database: 'myDB', sql: 'BEGIN'] as QueryEventData
        ))

        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.TABLE_MAP, nextPosition: 30] as EventHeaderV4,
                [tableId: 1, database: 'myDB', table: 'myTable', columnTypes: [4, -4] as byte[]] as TableMapEventData
        ))

        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 32] as EventHeaderV4,
                [database: 'myDB', sql: 'ALTER TABLE myTable add column col1 int'] as QueryEventData
        ))

        // DELETE scenario
        cols = new BitSet()
        cols.set(0)
        cols.set(1)
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.DELETE_ROWS, nextPosition: 36] as EventHeaderV4,
                [tableId: 1, includedColumns: cols, rows: [[2, 'Smith'] as Serializable[], [3, 'Jones'] as Serializable[]] as List<Serializable[]>] as DeleteRowsEventData
        ))

        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.XID, nextPosition: 40] as EventHeaderV4,
                {} as EventData
        ))

        testRunner.run(1, true, false)

        def resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS)
        List<String> expectedEventTypes = ([] + 'begin' + Collections.nCopies(3, 'insert') + 'commit' + 'begin' + 'update' + 'commit'
                + 'begin' + 'ddl' + Collections.nCopies(2, 'delete') + 'commit')

        resultFiles.eachWithIndex {e, i ->
            assertEquals(i, Long.valueOf(e.getAttribute(EventWriter.SEQUENCE_ID_KEY)))
            assertEquals(EventWriter.APPLICATION_JSON, e.getAttribute(CoreAttributes.MIME_TYPE.key()))
            assertEquals((i < 8) ? 'master.000001' : 'master.000002', e.getAttribute(BinlogEventInfo.BINLOG_FILENAME_KEY))
            assertTrue(Long.valueOf(e.getAttribute(BinlogEventInfo.BINLOG_POSITION_KEY)) % 4 == 0L)
            assertEquals(expectedEventTypes[i], e.getAttribute('cdc.event.type'))
            // Check that DDL didn't change
            if (e.getAttribute(BinlogEventInfo.BINLOG_POSITION_KEY) == "32") {
                assertEquals('ALTER TABLE myTable add column col1 int', new JsonSlurper().parse(testRunner.getContentAsByteArray(e)).query?.toString())
            }
        }
        assertEquals(13, resultFiles.size())
        assertEquals(13, testRunner.provenanceEvents.size())
        testRunner.provenanceEvents.each {assertEquals(ProvenanceEventType.RECEIVE, it.eventType)}
    }

    @Test
    void testExcludeSchemaChanges() throws Exception {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, 'file:///path/to/mysql-connector-java-5.1.38-bin.jar')
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, 'localhost:3306')
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, 'root')
        testRunner.setProperty(CaptureChangeMySQL.PASSWORD, 'password')
        testRunner.setProperty(CaptureChangeMySQL.SERVER_ID, '1')
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, '2 seconds')
        testRunner.setProperty(CaptureChangeMySQL.INIT_BINLOG_FILENAME, 'master.000001')
        testRunner.setProperty(CaptureChangeMySQL.INIT_BINLOG_POSITION, '4')
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_BEGIN_COMMIT, 'true')
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_DDL_EVENTS, 'false')
        final DistributedMapCacheClientImpl cacheClient = createCacheClient()
        def clientProperties = [:]
        clientProperties.put(DistributedMapCacheClientService.HOSTNAME.getName(), 'localhost')
        testRunner.addControllerService('client', cacheClient, clientProperties)
        testRunner.setProperty(CaptureChangeMySQL.DIST_CACHE_CLIENT, 'client')
        testRunner.enableControllerService(cacheClient)


        testRunner.run(1, false, true)

        // ROTATE scenario
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.ROTATE, nextPosition: 2] as EventHeaderV4,
                [binlogFilename: 'master.000001', binlogPosition: 4L] as RotateEventData
        ))

        // INSERT scenario
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 4] as EventHeaderV4,
                [database: 'myDB', sql: 'BEGIN'] as QueryEventData
        ))

        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.TABLE_MAP, nextPosition: 6] as EventHeaderV4,
                [tableId: 1, database: 'myDB', table: 'myTable', columnTypes: [4, -4] as byte[]] as TableMapEventData
        ))

        def cols = new BitSet()
        cols.set(1)
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.EXT_WRITE_ROWS, nextPosition: 8] as EventHeaderV4,
                [tableId: 1, includedColumns: cols,
                 rows   : [[2, 'Smith'] as Serializable[], [3, 'Jones'] as Serializable[], [10, 'Cruz'] as Serializable[]] as List<Serializable[]>] as WriteRowsEventData
        ))

        // ALTER TABLE
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 32] as EventHeaderV4,
                [database: 'myDB', sql: 'ALTER TABLE myTable add column col1 int'] as QueryEventData
        ))

        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.XID, nextPosition: 40] as EventHeaderV4,
                {} as EventData
        ))

        testRunner.run(1, true, false)

        def resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS)
        // No DDL events expected
        List<String> expectedEventTypes = ([] + 'begin' + Collections.nCopies(3, 'insert') + 'commit')

        resultFiles.eachWithIndex {e, i ->
            assertEquals(i, Long.valueOf(e.getAttribute(EventWriter.SEQUENCE_ID_KEY)))
            assertEquals(EventWriter.APPLICATION_JSON, e.getAttribute(CoreAttributes.MIME_TYPE.key()))
            assertEquals((i < 8) ? 'master.000001' : 'master.000002', e.getAttribute(BinlogEventInfo.BINLOG_FILENAME_KEY))
            assertTrue(Long.valueOf(e.getAttribute(BinlogEventInfo.BINLOG_POSITION_KEY)) % 4 == 0L)
            assertEquals(expectedEventTypes[i], e.getAttribute('cdc.event.type'))
        }
        assertEquals(5, resultFiles.size())
    }

    @Test(expected = AssertionError.class)
    void testNoTableInformationAvailable() throws Exception {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, 'file:///path/to/mysql-connector-java-5.1.38-bin.jar')
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, 'localhost:3306')
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, 'root')
        testRunner.setProperty(CaptureChangeMySQL.PASSWORD, 'password')
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, '2 seconds')
        final DistributedMapCacheClientImpl cacheClient = createCacheClient()
        def clientProperties = [:]
        clientProperties.put(DistributedMapCacheClientService.HOSTNAME.getName(), 'localhost')
        testRunner.addControllerService('client', cacheClient, clientProperties)
        testRunner.setProperty(CaptureChangeMySQL.DIST_CACHE_CLIENT, 'client')
        testRunner.enableControllerService(cacheClient)

        testRunner.run(1, false, true)

        // ROTATE scenario
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.ROTATE, nextPosition: 2] as EventHeaderV4,
                [binlogFilename: 'master.000001', binlogPosition: 4L] as RotateEventData
        ))

        // INSERT scenario
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 4] as EventHeaderV4,
                [database: 'myDB', sql: 'BEGIN'] as QueryEventData
        ))

        def cols = new BitSet()
        cols.set(1)
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.EXT_WRITE_ROWS, nextPosition: 8] as EventHeaderV4,
                [tableId: 1, includedColumns: cols,
                 rows   : [[2, 'Smith'] as Serializable[], [3, 'Jones'] as Serializable[], [10, 'Cruz'] as Serializable[]] as List<Serializable[]>] as WriteRowsEventData
        ))

        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.XID, nextPosition: 12] as EventHeaderV4,
                {} as EventData
        ))

        testRunner.run(1, true, false)
    }

    @Test
    void testSkipTable() throws Exception {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, 'file:///path/to/mysql-connector-java-5.1.38-bin.jar')
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, 'localhost:3306')
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, 'root')
        testRunner.setProperty(CaptureChangeMySQL.PASSWORD, 'password')
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, '2 seconds')
        testRunner.setProperty(CaptureChangeMySQL.DATABASE_NAME_PATTERN, "myDB")
        testRunner.setProperty(CaptureChangeMySQL.TABLE_NAME_PATTERN, "user")
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_BEGIN_COMMIT, 'true')

        testRunner.run(1, false, true)

        // ROTATE
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.ROTATE, nextPosition: 2] as EventHeaderV4,
                [binlogFilename: 'master.000001', binlogPosition: 4L] as RotateEventData
        ))

        // BEGIN
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 4] as EventHeaderV4,
                [database: 'myDB', sql: 'BEGIN'] as QueryEventData
        ))

        // TABLE MAP for table not matching the regex (note the s on the end of users vs the regex of 'user')
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.TABLE_MAP, nextPosition: 6] as EventHeaderV4,
                [tableId: 1, database: 'myDB', table: 'users', columnTypes: [4, -4] as byte[]] as TableMapEventData
        ))

        // This WRITE ROWS should be skipped
        def cols = new BitSet()
        cols.set(1)
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.EXT_WRITE_ROWS, nextPosition: 8] as EventHeaderV4,
                [tableId: 1, includedColumns: cols,
                 rows   : [[2, 'Smith'] as Serializable[]] as List<Serializable[]>] as WriteRowsEventData
        ))

        // TABLE MAP for table matching, all modification events (1) should be emitted
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.TABLE_MAP, nextPosition: 10] as EventHeaderV4,
                [tableId: 1, database: 'myDB', table: 'user', columnTypes: [4, -4] as byte[]] as TableMapEventData
        ))

        // WRITE ROWS for matching table
        cols = new BitSet()
        cols.set(1)
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.EXT_WRITE_ROWS, nextPosition: 12] as EventHeaderV4,
                [tableId: 1, includedColumns: cols,
                 rows   : [[10, 'Cruz'] as Serializable[]] as List<Serializable[]>] as WriteRowsEventData
        ))

        // COMMIT
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.XID, nextPosition: 14] as EventHeaderV4,
                {} as EventData
        ))

        ////////////////////////
        // Test database filter
        ////////////////////////

        // BEGIN
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 4] as EventHeaderV4,
                [database: 'myDB', sql: 'BEGIN'] as QueryEventData
        ))

        // TABLE MAP for database not matching the regex
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.TABLE_MAP, nextPosition: 6] as EventHeaderV4,
                [tableId: 1, database: 'myDB', table: 'notMyDB', columnTypes: [4, -4] as byte[]] as TableMapEventData
        ))

        // This WRITE ROWS should be skipped
        cols = new BitSet()
        cols.set(1)
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.EXT_WRITE_ROWS, nextPosition: 8] as EventHeaderV4,
                [tableId: 1, includedColumns: cols,
                 rows   : [[2, 'Smith'] as Serializable[]] as List<Serializable[]>] as WriteRowsEventData
        ))

        // COMMIT
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.XID, nextPosition: 14] as EventHeaderV4,
                {} as EventData
        ))

        testRunner.run(1, true, false)

        def resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS)
        // BEGIN + WRITE + COMMIT from table matching, BEGIN + COMMIT for database matching
        assertEquals(5, resultFiles.size())
    }

    @Test
    void testFilterDatabase() throws Exception {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, 'file:///path/to/mysql-connector-java-5.1.38-bin.jar')
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, 'localhost:3306')
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, 'root')
        testRunner.setProperty(CaptureChangeMySQL.PASSWORD, 'password')
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, '2 seconds')
        testRunner.setProperty(CaptureChangeMySQL.DATABASE_NAME_PATTERN, "myDB")
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_BEGIN_COMMIT, 'true')
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_DDL_EVENTS, 'true')

        testRunner.run(1, false, true)

        // ROTATE
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.ROTATE, nextPosition: 2] as EventHeaderV4,
                [binlogFilename: 'master.000001', binlogPosition: 4L] as RotateEventData
        ))

        // BEGIN
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 4] as EventHeaderV4,
                [database: 'myDB', sql: 'BEGIN'] as QueryEventData
        ))

        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 32] as EventHeaderV4,
                [database: 'myDB', sql: 'ALTER TABLE myTable add column col1 int'] as QueryEventData
        ))

        // COMMIT
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.XID, nextPosition: 14] as EventHeaderV4,
                {} as EventData
        ))

        ////////////////////////
        // Test database filter
        ////////////////////////

        // BEGIN
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 4] as EventHeaderV4,
                [database: 'NotMyDB', sql: 'BEGIN'] as QueryEventData
        ))

        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 32] as EventHeaderV4,
                [database: 'NotMyDB', sql: 'ALTER TABLE myTable add column col1 int'] as QueryEventData
        ))

        // COMMIT
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.XID, nextPosition: 14] as EventHeaderV4,
                {} as EventData
        ))

        testRunner.run(1, true, false)

        def resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS)
        // First BEGIN + DDL + COMMIT only
        assertEquals(3, resultFiles.size())

        // Check that the database name is set on the objects
        resultFiles.each {f ->
            def json = new JsonSlurper().parseText(new String(f.toByteArray()))
            assertEquals('myDB', json.database)
        }
    }

    @Test
    void testTransactionAcrossMultipleProcessorExecutions() throws Exception {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, 'file:///path/to/mysql-connector-java-5.1.38-bin.jar')
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, 'localhost:3306')
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, 'root')
        testRunner.setProperty(CaptureChangeMySQL.PASSWORD, 'password')
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, '2 seconds')
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_BEGIN_COMMIT, 'true')

        testRunner.run(1, false, true)

        // ROTATE
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.ROTATE, nextPosition: 2] as EventHeaderV4,
                [binlogFilename: 'master.000001', binlogPosition: 4L] as RotateEventData
        ))

        // BEGIN
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 4] as EventHeaderV4,
                [database: 'myDB', sql: 'BEGIN'] as QueryEventData
        ))

        // TABLE MAP
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.TABLE_MAP, nextPosition: 6] as EventHeaderV4,
                [tableId: 1, database: 'myDB', table: 'users', columnTypes: [4, -4] as byte[]] as TableMapEventData
        ))

        // Run and Stop the processor
        testRunner.run(1, true, false)

        def resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS)
        assertEquals(1, resultFiles.size())

        // Re-initialize the processor so it can receive events
        testRunner.run(1, false, true)

        // This WRITE ROWS should be skipped
        def cols = new BitSet()
        cols.set(1)
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.EXT_WRITE_ROWS, nextPosition: 8] as EventHeaderV4,
                [tableId: 1, includedColumns: cols,
                 rows   : [[2, 'Smith'] as Serializable[]] as List<Serializable[]>] as WriteRowsEventData
        ))

        // COMMIT
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.XID, nextPosition: 14] as EventHeaderV4,
                {} as EventData
        ))

        // Run and Stop the processor
        testRunner.run(1, true, false)


        resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS)
        assertEquals(3, resultFiles.size())
    }

    @Test
    void testUpdateState() throws Exception {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, 'file:///path/to/mysql-connector-java-5.1.38-bin.jar')
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, 'localhost:3306')
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, 'root')
        testRunner.setProperty(CaptureChangeMySQL.PASSWORD, 'password')
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, '2 seconds')

        testRunner.run(1, false, true)

        // ROTATE
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.ROTATE, nextPosition: 2] as EventHeaderV4,
                [binlogFilename: 'master.000001', binlogPosition: 4L] as RotateEventData
        ))

        testRunner.run(1, false, false)

        // Ensure state not set, as the processor hasn't been stopped and no State Update Interval has been set
        testRunner.stateManager.assertStateEquals(BinlogEventInfo.BINLOG_FILENAME_KEY, null, Scope.CLUSTER)
        testRunner.stateManager.assertStateEquals(BinlogEventInfo.BINLOG_POSITION_KEY, null, Scope.CLUSTER)

        // Stop the processor and verify the state is set
        testRunner.run(1, true, false)
        testRunner.stateManager.assertStateEquals(BinlogEventInfo.BINLOG_FILENAME_KEY, 'master.000001', Scope.CLUSTER)
        testRunner.stateManager.assertStateEquals(BinlogEventInfo.BINLOG_POSITION_KEY, '4', Scope.CLUSTER)

        testRunner.stateManager.clear(Scope.CLUSTER)

        // Send some events, wait for the State Update Interval, and verify the state was set
        testRunner.setProperty(CaptureChangeMySQL.STATE_UPDATE_INTERVAL, '1 second')
        testRunner.run(1, false, true)

        // ROTATE
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.ROTATE, nextPosition: 2] as EventHeaderV4,
                [binlogFilename: 'master.000001', binlogPosition: 4L] as RotateEventData
        ))

        // BEGIN
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 6] as EventHeaderV4,
                [database: 'myDB', sql: 'BEGIN'] as QueryEventData
        ))

        sleep(1000)

        testRunner.run(1, false, false)

        testRunner.stateManager.assertStateEquals(BinlogEventInfo.BINLOG_FILENAME_KEY, 'master.000001', Scope.CLUSTER)
        testRunner.stateManager.assertStateEquals(BinlogEventInfo.BINLOG_POSITION_KEY, '6', Scope.CLUSTER)

        // COMMIT
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.XID, nextPosition: 12] as EventHeaderV4,
                {} as EventData
        ))

        testRunner.run(1, true, false)

        testRunner.stateManager.assertStateEquals(BinlogEventInfo.BINLOG_FILENAME_KEY, 'master.000001', Scope.CLUSTER)
        testRunner.stateManager.assertStateEquals(BinlogEventInfo.BINLOG_POSITION_KEY, '12', Scope.CLUSTER)

    }

    @Test
    void testDDLOutsideTransaction() throws Exception {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, 'file:///path/to/mysql-connector-java-5.1.38-bin.jar')
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, 'localhost:3306')
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, 'root')
        testRunner.setProperty(CaptureChangeMySQL.PASSWORD, 'password')
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, '2 seconds')
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_DDL_EVENTS, 'true')

        testRunner.run(1, false, true)

        // ROTATE
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.ROTATE, nextPosition: 2] as EventHeaderV4,
                [binlogFilename: 'master.000001', binlogPosition: 4L] as RotateEventData
        ))

        // DROP TABLE
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 4] as EventHeaderV4,
                [database: 'myDB', sql: 'DROP TABLE myTable'] as QueryEventData
        ))

        testRunner.run(1, false, false)
        testRunner.assertTransferCount(CaptureChangeMySQL.REL_SUCCESS, 1)
    }

    @Test
    void testRenameTable() throws Exception {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, 'file:///path/to/mysql-connector-java-5.1.38-bin.jar')
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, 'localhost:3306')
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, 'root')
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, '2 seconds')
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_DDL_EVENTS, 'true')

        testRunner.run(1, false, true)

        // ROTATE
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.ROTATE, nextPosition: 2] as EventHeaderV4,
                [binlogFilename: 'master.000001', binlogPosition: 4L] as RotateEventData
        ))

        // BEGIN
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 4] as EventHeaderV4,
                [database: 'myDB', sql: 'BEGIN'] as QueryEventData
        ))

        // RENAME TABLE
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 4] as EventHeaderV4,
                [database: 'myDB', sql: 'RENAME TABLE myTable TO myTable2'] as QueryEventData
        ))

        // COMMIT
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.XID, nextPosition: 12] as EventHeaderV4,
                {} as EventData
        ))

        testRunner.run(1, true, false)

        def resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS)
        assertEquals(1, resultFiles.size())
    }

    /********************************
     * Mock and helper classes below
     ********************************/

    class MockCaptureChangeMySQL extends CaptureChangeMySQL {

        Map<TableInfoCacheKey, TableInfo> cache = new HashMap<>()

        @Override
        BinaryLogClient createBinlogClient(String hostname, int port, String username, String password) {
            client
        }

        @Override
        protected TableInfo loadTableInfo(TableInfoCacheKey key) {
            TableInfo tableInfo = cache.get(key)
            if (tableInfo == null) {
                tableInfo = new TableInfo(key.databaseName, key.tableName, key.tableId,
                        [new ColumnDefinition((byte) 4, 'id'),
                         new ColumnDefinition((byte) -4, 'string1')
                        ] as List<ColumnDefinition>)
                cache.put(key, tableInfo)
            }
            return tableInfo
        }

        @Override
        protected void registerDriver(String locationString, String drvName) throws InitializationException {
        }

        @Override
        protected Connection getJdbcConnection() throws SQLException {
            Connection mockConnection = mock(Connection)
            Statement mockStatement = mock(Statement)
            when(mockConnection.createStatement()).thenReturn(mockStatement)
            ResultSet mockResultSet = mock(ResultSet)
            when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet)
            return mockConnection
        }
    }


    static DistributedMapCacheClientImpl createCacheClient() throws InitializationException {

        final DistributedMapCacheClientImpl client = new DistributedMapCacheClientImpl()
        final ComponentLog logger = new MockComponentLog("client", client)
        final MockControllerServiceInitializationContext clientInitContext = new MockControllerServiceInitializationContext(client, "client", logger, new MockStateManager(client))

        client.initialize(clientInitContext)

        return client
    }

    static
    final class DistributedMapCacheClientImpl extends AbstractControllerService implements DistributedMapCacheClient {

        private Map<String, String> cacheMap = new HashMap<>()

        @Override
        void close() throws IOException {
        }

        @Override
        void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        }

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {

            return [DistributedMapCacheClientService.HOSTNAME,
                    DistributedMapCacheClientService.COMMUNICATIONS_TIMEOUT,
                    DistributedMapCacheClientService.PORT,
                    DistributedMapCacheClientService.SSL_CONTEXT_SERVICE]
        }

        @Override
        <K, V> boolean putIfAbsent(
                final K key,
                final V value,
                final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {

            StringWriter keyWriter = new StringWriter()
            keySerializer.serialize(key, new WriterOutputStream(keyWriter))
            String keyString = keyWriter.toString()

            if (cacheMap.containsKey(keyString)) return false

            StringWriter valueWriter = new StringWriter()
            valueSerializer.serialize(value, new WriterOutputStream(valueWriter))
            return true
        }

        @Override
        @SuppressWarnings("unchecked")
        <K, V> V getAndPutIfAbsent(
                final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer,
                final Deserializer<V> valueDeserializer) throws IOException {
            StringWriter keyWriter = new StringWriter()
            keySerializer.serialize(key, new WriterOutputStream(keyWriter))
            String keyString = keyWriter.toString()

            if (cacheMap.containsKey(keyString)) return valueDeserializer.deserialize(cacheMap.get(keyString).bytes)

            StringWriter valueWriter = new StringWriter()
            valueSerializer.serialize(value, new WriterOutputStream(valueWriter))
            return null
        }

        @Override
        <K> boolean containsKey(final K key, final Serializer<K> keySerializer) throws IOException {
            StringWriter keyWriter = new StringWriter()
            keySerializer.serialize(key, new WriterOutputStream(keyWriter))
            String keyString = keyWriter.toString()

            return cacheMap.containsKey(keyString)
        }

        @Override
        <K, V> V get(
                final K key,
                final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {
            StringWriter keyWriter = new StringWriter()
            keySerializer.serialize(key, new WriterOutputStream(keyWriter))
            String keyString = keyWriter.toString()

            return (cacheMap.containsKey(keyString)) ? valueDeserializer.deserialize(cacheMap.get(keyString).bytes) : null
        }

        @Override
        <K> boolean remove(final K key, final Serializer<K> serializer) throws IOException {
            StringWriter keyWriter = new StringWriter()
            serializer.serialize(key, new WriterOutputStream(keyWriter))
            String keyString = keyWriter.toString()

            boolean removed = (cacheMap.containsKey(keyString))
            cacheMap.remove(keyString)
            return removed
        }

        @Override
        long removeByPattern(String regex) throws IOException {
            final List<String> removedRecords = new ArrayList<>()
            Pattern p = Pattern.compile(regex)
            for (String key : cacheMap.keySet()) {
                // Key must be backed by something that can be converted into a String
                Matcher m = p.matcher(key)
                if (m.matches()) {
                    removedRecords.add(cacheMap.get(key))
                }
            }
            final long numRemoved = removedRecords.size()
            removedRecords.each {cacheMap.remove(it)}
            return numRemoved
        }

        @Override
        <K, V> void put(
                final K key,
                final V value,
                final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
            StringWriter keyWriter = new StringWriter()
            keySerializer.serialize(key, new WriterOutputStream(keyWriter))
            StringWriter valueWriter = new StringWriter()
            valueSerializer.serialize(value, new WriterOutputStream(valueWriter))
        }
    }
}
