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
package org.apache.nifi.processors.hadoop.inotify;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.processors.hadoop.inotify.util.EventTestUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestGetHDFSEvents {
    NiFiProperties mockNiFiProperties;
    KerberosProperties kerberosProperties;
    DFSInotifyEventInputStream inotifyEventInputStream;
    HdfsAdmin hdfsAdmin;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setup() {
        mockNiFiProperties = mock(NiFiProperties.class);
        when(mockNiFiProperties.getKerberosConfigurationFile()).thenReturn(null);
        kerberosProperties = new KerberosProperties(null);
        inotifyEventInputStream = mock(DFSInotifyEventInputStream.class);
        hdfsAdmin = mock(HdfsAdmin.class);
    }

    @Test
    public void notSettingHdfsPathToWatchShouldThrowError() throws Exception {
        exception.expect(AssertionError.class);
        exception.expectMessage("'HDFS Path to Watch' is invalid because HDFS Path to Watch is required");

        GetHDFSEvents processor = new TestableGetHDFSEvents(kerberosProperties, hdfsAdmin);
        TestRunner runner = TestRunners.newTestRunner(processor);

        runner.setProperty(GetHDFSEvents.POLL_DURATION, "1 second");
        runner.run();
    }

    @Test
    public void onTriggerShouldProperlyHandleAnEmptyEventBatch() throws Exception {
        EventBatch eventBatch = mock(EventBatch.class);
        when(eventBatch.getEvents()).thenReturn(new Event[]{});

        when(inotifyEventInputStream.poll(1000000L, TimeUnit.MICROSECONDS)).thenReturn(eventBatch);
        when(hdfsAdmin.getInotifyEventStream()).thenReturn(inotifyEventInputStream);
        when(eventBatch.getTxid()).thenReturn(100L);

        GetHDFSEvents processor = new TestableGetHDFSEvents(kerberosProperties, hdfsAdmin);
        TestRunner runner = TestRunners.newTestRunner(processor);

        runner.setProperty(GetHDFSEvents.POLL_DURATION, "1 second");
        runner.setProperty(GetHDFSEvents.HDFS_PATH_TO_WATCH, "/some/path");
        runner.setProperty(GetHDFSEvents.NUMBER_OF_RETRIES_FOR_POLL, "5");
        runner.run();

        List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(GetHDFSEvents.REL_SUCCESS);
        assertEquals(0, successfulFlowFiles.size());
        verify(eventBatch).getTxid();
        assertEquals("100", runner.getProcessContext().getStateManager().getState(Scope.CLUSTER).get("last.tx.id"));
    }

    @Test
    public void onTriggerShouldProperlyHandleANullEventBatch() throws Exception {
        when(inotifyEventInputStream.poll(1000000L, TimeUnit.MICROSECONDS)).thenReturn(null);
        when(hdfsAdmin.getInotifyEventStream()).thenReturn(inotifyEventInputStream);

        GetHDFSEvents processor = new TestableGetHDFSEvents(kerberosProperties, hdfsAdmin);
        TestRunner runner = TestRunners.newTestRunner(processor);

        runner.setProperty(GetHDFSEvents.POLL_DURATION, "1 second");
        runner.setProperty(GetHDFSEvents.HDFS_PATH_TO_WATCH, "/some/path${now()}");
        runner.run();

        List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(GetHDFSEvents.REL_SUCCESS);
        assertEquals(0, successfulFlowFiles.size());
        assertEquals("-1", runner.getProcessContext().getStateManager().getState(Scope.CLUSTER).get("last.tx.id"));
    }

    @Test
    public void makeSureHappyPathForProcessingEventsSendsFlowFilesToCorrectRelationship() throws Exception {
        Event[] events = getEvents();

        EventBatch eventBatch = mock(EventBatch.class);
        when(eventBatch.getEvents()).thenReturn(events);

        when(inotifyEventInputStream.poll(1000000L, TimeUnit.MICROSECONDS)).thenReturn(eventBatch);
        when(hdfsAdmin.getInotifyEventStream()).thenReturn(inotifyEventInputStream);
        when(eventBatch.getTxid()).thenReturn(100L);

        GetHDFSEvents processor = new TestableGetHDFSEvents(kerberosProperties, hdfsAdmin);
        TestRunner runner = TestRunners.newTestRunner(processor);

        runner.setProperty(GetHDFSEvents.POLL_DURATION, "1 second");
        runner.setProperty(GetHDFSEvents.HDFS_PATH_TO_WATCH, "/some/path(/)?.*");
        runner.run();

        List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(GetHDFSEvents.REL_SUCCESS);
        assertEquals(3, successfulFlowFiles.size());
        verify(eventBatch).getTxid();
        assertEquals("100", runner.getProcessContext().getStateManager().getState(Scope.CLUSTER).get("last.tx.id"));
    }

    @Test
    public void onTriggerShouldOnlyProcessEventsWithSpecificPath() throws Exception {
        Event[] events = getEvents();

        EventBatch eventBatch = mock(EventBatch.class);
        when(eventBatch.getEvents()).thenReturn(events);

        when(inotifyEventInputStream.poll(1000000L, TimeUnit.MICROSECONDS)).thenReturn(eventBatch);
        when(hdfsAdmin.getInotifyEventStream()).thenReturn(inotifyEventInputStream);
        when(eventBatch.getTxid()).thenReturn(100L);

        GetHDFSEvents processor = new TestableGetHDFSEvents(kerberosProperties, hdfsAdmin);
        TestRunner runner = TestRunners.newTestRunner(processor);

        runner.setProperty(GetHDFSEvents.HDFS_PATH_TO_WATCH, "/some/path/create(/)?");
        runner.run();

        List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(GetHDFSEvents.REL_SUCCESS);
        assertEquals(1, successfulFlowFiles.size());
        verify(eventBatch).getTxid();
        assertEquals("100", runner.getProcessContext().getStateManager().getState(Scope.CLUSTER).get("last.tx.id"));
    }

    @Test
    public void eventsProcessorShouldProperlyFilterEventTypes() throws Exception {
        Event[] events = getEvents();

        EventBatch eventBatch = mock(EventBatch.class);
        when(eventBatch.getEvents()).thenReturn(events);

        when(inotifyEventInputStream.poll(1000000L, TimeUnit.MICROSECONDS)).thenReturn(eventBatch);
        when(hdfsAdmin.getInotifyEventStream()).thenReturn(inotifyEventInputStream);
        when(eventBatch.getTxid()).thenReturn(100L);

        GetHDFSEvents processor = new TestableGetHDFSEvents(kerberosProperties, hdfsAdmin);
        TestRunner runner = TestRunners.newTestRunner(processor);

        runner.setProperty(GetHDFSEvents.HDFS_PATH_TO_WATCH, "/some/path(/.*)?");
        runner.setProperty(GetHDFSEvents.EVENT_TYPES, "create, metadata");
        runner.run();

        List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(GetHDFSEvents.REL_SUCCESS);
        assertEquals(2, successfulFlowFiles.size());

        List<String> expectedEventTypes = Arrays.asList("CREATE", "METADATA");
        for (MockFlowFile f : successfulFlowFiles) {
            String eventType = f.getAttribute(EventAttributes.EVENT_TYPE);
            assertTrue(expectedEventTypes.contains(eventType));
        }

        verify(eventBatch).getTxid();
        assertEquals("100", runner.getProcessContext().getStateManager().getState(Scope.CLUSTER).get("last.tx.id"));
    }

    @Test
    public void makeSureExpressionLanguageIsWorkingProperlyWithinTheHdfsPathToWatch() throws Exception {
        Event[] events = new Event[] {
                new Event.CreateEvent.Builder().path("/some/path/1/2/3/t.txt").build(),
                new Event.CreateEvent.Builder().path("/some/path/1/2/4/t.txt").build(),
                new Event.CreateEvent.Builder().path("/some/path/1/2/3/.t.txt").build()
        };

        EventBatch eventBatch = mock(EventBatch.class);
        when(eventBatch.getEvents()).thenReturn(events);

        when(inotifyEventInputStream.poll(1000000L, TimeUnit.MICROSECONDS)).thenReturn(eventBatch);
        when(hdfsAdmin.getInotifyEventStream()).thenReturn(inotifyEventInputStream);
        when(eventBatch.getTxid()).thenReturn(100L);

        GetHDFSEvents processor = new TestableGetHDFSEvents(kerberosProperties, hdfsAdmin);
        TestRunner runner = TestRunners.newTestRunner(processor);

        runner.setProperty(GetHDFSEvents.HDFS_PATH_TO_WATCH, "/some/path/${literal(1)}/${literal(2)}/${literal(3)}/.*.txt");
        runner.setProperty(GetHDFSEvents.EVENT_TYPES, "create");
        runner.setProperty(GetHDFSEvents.IGNORE_HIDDEN_FILES, "true");
        runner.run();

        List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(GetHDFSEvents.REL_SUCCESS);
        assertEquals(1, successfulFlowFiles.size());

        for (MockFlowFile f : successfulFlowFiles) {
            String eventType = f.getAttribute(EventAttributes.EVENT_TYPE);
            assertTrue(eventType.equals("CREATE"));
        }

        verify(eventBatch).getTxid();
        assertEquals("100", runner.getProcessContext().getStateManager().getState(Scope.CLUSTER).get("last.tx.id"));
    }

    private Event[] getEvents() {
        return new Event[]{
                    EventTestUtils.createCreateEvent(),
                    EventTestUtils.createCloseEvent(),
                    EventTestUtils.createMetadataUpdateEvent()
            };
    }

    private class TestableGetHDFSEvents extends GetHDFSEvents {

        private final KerberosProperties testKerberosProperties;
        private final FileSystem fileSystem = new DistributedFileSystem();
        private final HdfsAdmin hdfsAdmin;

        TestableGetHDFSEvents(KerberosProperties testKerberosProperties, HdfsAdmin hdfsAdmin) {
            this.testKerberosProperties = testKerberosProperties;
            this.hdfsAdmin = hdfsAdmin;
        }

        @Override
        protected FileSystem getFileSystem() {
            return fileSystem;
        }

        @Override
        protected KerberosProperties getKerberosProperties() {
            return testKerberosProperties;
        }

        @Override
        protected HdfsAdmin getHdfsAdmin() {
            return hdfsAdmin;
        }
    }
}
