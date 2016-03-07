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
package org.apache.nifi.processors.splunk;

import com.splunk.JobExportArgs;
import com.splunk.Service;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestGetSplunk {

    private Service service;
    private TestableGetSplunk proc;
    private TestRunner runner;

    @Before
    public void setup() {
        service = Mockito.mock(Service.class);
        proc = new TestableGetSplunk(service);

        runner = TestRunners.newTestRunner(proc);
    }

    @Test
    public void testCustomValidation() {
        final String query = "search tcp:7879";
        final String providedEarliest = "-1h";
        final String providedLatest = "now";
        final String outputMode = GetSplunk.ATOM_VALUE.getValue();

        runner.setProperty(GetSplunk.QUERY, query);
        runner.setProperty(GetSplunk.EARLIEST_TIME, providedEarliest);
        runner.setProperty(GetSplunk.LATEST_TIME, providedLatest);
        runner.setProperty(GetSplunk.OUTPUT_MODE, outputMode);
        runner.assertValid();

        runner.setProperty(GetSplunk.USERNAME, "user1");
        runner.assertNotValid();

        runner.setProperty(GetSplunk.PASSWORD, "password");
        runner.assertValid();
    }

    @Test
    public void testGetWithProvidedTime() {
        final String query = "search tcp:7879";
        final String providedEarliest = "-1h";
        final String providedLatest = "now";
        final String outputMode = GetSplunk.ATOM_VALUE.getValue();

        runner.setProperty(GetSplunk.QUERY, query);
        runner.setProperty(GetSplunk.EARLIEST_TIME, providedEarliest);
        runner.setProperty(GetSplunk.LATEST_TIME, providedLatest);
        runner.setProperty(GetSplunk.OUTPUT_MODE, outputMode);

        final JobExportArgs expectedArgs = new JobExportArgs();
        expectedArgs.setSearchMode(JobExportArgs.SearchMode.NORMAL);
        expectedArgs.setEarliestTime(providedEarliest);
        expectedArgs.setLatestTime(providedLatest);
        expectedArgs.setOutputMode(JobExportArgs.OutputMode.valueOf(outputMode));

        final String resultContent = "fake results";
        final ByteArrayInputStream input = new ByteArrayInputStream(resultContent.getBytes(StandardCharsets.UTF_8));
        when(service.export(eq(query), argThat(new JobExportArgsMatcher(expectedArgs)))).thenReturn(input);

        runner.run();
        runner.assertAllFlowFilesTransferred(GetSplunk.REL_SUCCESS, 1);

        final List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(GetSplunk.REL_SUCCESS);
        Assert.assertEquals(1, mockFlowFiles.size());

        final MockFlowFile mockFlowFile = mockFlowFiles.get(0);
        mockFlowFile.assertContentEquals(resultContent);
        mockFlowFile.assertAttributeEquals(GetSplunk.QUERY_ATTR, query);
        mockFlowFile.assertAttributeEquals(GetSplunk.EARLIEST_TIME_ATTR, providedEarliest);
        mockFlowFile.assertAttributeEquals(GetSplunk.LATEST_TIME_ATTR, providedLatest);
        Assert.assertEquals(1, proc.count);

        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        Assert.assertEquals(1, events.size());
        Assert.assertEquals(ProvenanceEventType.RECEIVE, events.get(0).getEventType());
        Assert.assertEquals("https://localhost:8089", events.get(0).getTransitUri());
    }

    @Test
    public void testMultipleIterationsWithoutShuttingDown() {
        final String query = "search tcp:7879";
        final String providedEarliest = "-1h";
        final String providedLatest = "now";
        final String outputMode = GetSplunk.ATOM_VALUE.getValue();

        runner.setProperty(GetSplunk.QUERY, query);
        runner.setProperty(GetSplunk.EARLIEST_TIME, providedEarliest);
        runner.setProperty(GetSplunk.LATEST_TIME, providedLatest);
        runner.setProperty(GetSplunk.OUTPUT_MODE, outputMode);

        final JobExportArgs expectedArgs = new JobExportArgs();
        expectedArgs.setSearchMode(JobExportArgs.SearchMode.NORMAL);
        expectedArgs.setEarliestTime(providedEarliest);
        expectedArgs.setLatestTime(providedLatest);
        expectedArgs.setOutputMode(JobExportArgs.OutputMode.valueOf(outputMode));

        final String resultContent = "fake results";
        final ByteArrayInputStream input = new ByteArrayInputStream(resultContent.getBytes(StandardCharsets.UTF_8));
        when(service.export(eq(query), argThat(new JobExportArgsMatcher(expectedArgs)))).thenReturn(input);

        final int iterations = 3;
        runner.run(iterations, false);
        runner.assertAllFlowFilesTransferred(GetSplunk.REL_SUCCESS, iterations);
        Assert.assertEquals(1, proc.count);
    }

    @Test
    public void testGetWithManagedFromBeginning() {
        final String query = "search tcp:7879";
        final String outputMode = GetSplunk.ATOM_VALUE.getValue();

        runner.setProperty(GetSplunk.QUERY, query);
        runner.setProperty(GetSplunk.OUTPUT_MODE, outputMode);
        runner.setProperty(GetSplunk.TIME_RANGE_STRATEGY, GetSplunk.MANAGED_BEGINNING_VALUE.getValue());

        final String resultContent = "fake results";
        final ByteArrayInputStream input = new ByteArrayInputStream(resultContent.getBytes(StandardCharsets.UTF_8));
        when(service.export(eq(query), any(JobExportArgs.class))).thenReturn(input);

        // run once and don't shut down
        runner.run(1, false);
        runner.assertAllFlowFilesTransferred(GetSplunk.REL_SUCCESS, 1);

        // capture what the args were on last run
        final ArgumentCaptor<JobExportArgs> capture1 = ArgumentCaptor.forClass(JobExportArgs.class);
        verify(service, times(1)).export(eq(query), capture1.capture());

        // first execution with no previous state and "managed from beginning" should have a latest time and no earliest time
        final JobExportArgs actualArgs1 = capture1.getValue();
        Assert.assertNotNull(actualArgs1);
        Assert.assertNull(actualArgs1.get("earliest_time"));
        Assert.assertNotNull(actualArgs1.get("latest_time"));

        // save the latest time from the first run which should be earliest time of next run
        final String expectedLatest = (String) actualArgs1.get("latest_time");

        // run again
        runner.run(1, false);
        runner.assertAllFlowFilesTransferred(GetSplunk.REL_SUCCESS, 2);

        final ArgumentCaptor<JobExportArgs> capture2 = ArgumentCaptor.forClass(JobExportArgs.class);
        verify(service, times(2)).export(eq(query), capture2.capture());

        // second execution the earliest time should be the previous latest_time
        final JobExportArgs actualArgs2 = capture2.getValue();
        Assert.assertNotNull(actualArgs2);
        Assert.assertEquals(expectedLatest, actualArgs2.get("earliest_time"));
        Assert.assertNotNull(actualArgs2.get("latest_time"));
    }

    @Test
    public void testGetWithManagedFromCurrent() throws IOException {
        final String query = "search tcp:7879";
        final String outputMode = GetSplunk.ATOM_VALUE.getValue();

        runner.setProperty(GetSplunk.QUERY, query);
        runner.setProperty(GetSplunk.OUTPUT_MODE, outputMode);
        runner.setProperty(GetSplunk.TIME_RANGE_STRATEGY, GetSplunk.MANAGED_CURRENT_VALUE.getValue());

        final String resultContent = "fake results";
        final ByteArrayInputStream input = new ByteArrayInputStream(resultContent.getBytes(StandardCharsets.UTF_8));
        when(service.export(eq(query), any(JobExportArgs.class))).thenReturn(input);

        // run once and don't shut down, shouldn't produce any results first time
        runner.run(1, false);
        runner.assertAllFlowFilesTransferred(GetSplunk.REL_SUCCESS, 0);

        // capture what the args were on last run
        verify(service, times(0)).export(eq(query), any(JobExportArgs.class));

        final StateMap state = runner.getStateManager().getState(Scope.CLUSTER);
        Assert.assertNotNull(state);
        Assert.assertTrue(state.getVersion() > 0);

        // save the latest time from the first run which should be earliest time of next run
        final String expectedLatest = state.get(GetSplunk.LATEST_TIME_KEY);

        // run again
        runner.run(1, false);
        runner.assertAllFlowFilesTransferred(GetSplunk.REL_SUCCESS, 1);

        final ArgumentCaptor<JobExportArgs> capture = ArgumentCaptor.forClass(JobExportArgs.class);
        verify(service, times(1)).export(eq(query), capture.capture());

        // second execution the earliest time should be the previous latest_time
        final JobExportArgs actualArgs = capture.getValue();
        Assert.assertNotNull(actualArgs);
        Assert.assertEquals(expectedLatest, actualArgs.get("earliest_time"));
        Assert.assertNotNull(actualArgs.get("latest_time"));
    }


    /**
     * Testable implementation of GetSplunk to return a Mock Splunk Service.
     */
    private static class TestableGetSplunk extends GetSplunk {

        int count;
        Service mockService;

        public TestableGetSplunk(Service mockService) {
            this.mockService = mockService;
        }

        @Override
        protected Service createSplunkService(ProcessContext context) {
            count++;
            return mockService;
        }
    }

    /**
     * Custom args matcher for JobExportArgs.
     */
    private static class JobExportArgsMatcher extends ArgumentMatcher<JobExportArgs> {

        private JobExportArgs expected;

        public JobExportArgsMatcher(JobExportArgs expected) {
            this.expected = expected;
        }

        @Override
        public boolean matches(Object o) {
            if (o == null) {
                return false;
            }

            if (!(o instanceof JobExportArgs)) {
                return false;
            }

            JobExportArgs other = (JobExportArgs) o;
            return expected.equals(other);
        }

    }

}
