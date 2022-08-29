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
package org.apache.nifi.processors.gcp.bigquery;

import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link PutBigQueryBatch}.
 */
public class PutBigQueryBatchTest extends AbstractBQTest {
    private static final String TABLE_NAME = "test_table";
    private static final String TABLE_SCHEMA = "[{ \"mode\": \"NULLABLE\", \"name\": \"data\", \"type\": \"STRING\" }]";
    private static final String SOURCE_TYPE = FormatOptions.json().getType();
    private static final String CREATE_DISPOSITION = JobInfo.CreateDisposition.CREATE_IF_NEEDED.name();
    private static final String WRITE_DISPOSITION = JobInfo.WriteDisposition.WRITE_EMPTY.name();
    private static final String MAX_BAD_RECORDS = "0";
    private static final String IGNORE_UNKNOWN = "true";
    private static final String READ_TIMEOUT = "5 minutes";

    @Mock
    Job job;

    @Mock
    JobId jobId;

    @Mock
    JobStatus jobStatus;

    @Mock
    JobStatistics stats;

    @Mock
    TableDataWriteChannel tableDataWriteChannel;

    @Override
    public AbstractBigQueryProcessor getProcessor() {
        return new PutBigQueryBatch() {
            @Override
            protected BigQuery getCloudService() {
                return bq;
            }

            @Override
            protected BigQuery getCloudService(final ProcessContext context) {
                return bq;
            }
        };
    }

    @Override
    protected void addRequiredPropertiesToRunner(TestRunner runner) {
        runner.setProperty(PutBigQueryBatch.DATASET, DATASET);
        runner.setProperty(PutBigQueryBatch.TABLE_NAME, TABLE_NAME);
        runner.setProperty(PutBigQueryBatch.TABLE_SCHEMA, TABLE_SCHEMA);
        runner.setProperty(PutBigQueryBatch.SOURCE_TYPE, SOURCE_TYPE);
        runner.setProperty(PutBigQueryBatch.CREATE_DISPOSITION, CREATE_DISPOSITION);
        runner.setProperty(PutBigQueryBatch.WRITE_DISPOSITION, WRITE_DISPOSITION);
        runner.setProperty(PutBigQueryBatch.MAXBAD_RECORDS, MAX_BAD_RECORDS);
        runner.setProperty(PutBigQueryBatch.IGNORE_UNKNOWN, IGNORE_UNKNOWN);
        runner.setProperty(PutBigQueryBatch.READ_TIMEOUT, READ_TIMEOUT);
    }

    @Test
    public void testSuccessfulLoad() throws Exception {
        when(bq.writer(ArgumentMatchers.isA(WriteChannelConfiguration.class))).thenReturn(tableDataWriteChannel);
        when(tableDataWriteChannel.getJob()).thenReturn(job);
        when(job.waitFor(ArgumentMatchers.isA(RetryOption.class))).thenReturn(job);
        when(job.getStatus()).thenReturn(jobStatus);
        when(job.getStatistics()).thenReturn(stats);

        when(stats.getCreationTime()).thenReturn(0L);
        when(stats.getStartTime()).thenReturn(1L);
        when(stats.getEndTime()).thenReturn(2L);
        when(job.getJobId()).thenReturn(jobId);
        when(jobId.getJob()).thenReturn("job-id");

        final AbstractBigQueryProcessor processor = getProcessor();
        final TestRunner runner = buildNewRunner(processor);
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        runner.enqueue("{ \"data\": \"datavalue\" }");

        runner.run();

        when(bq.testIamPermissions(any(), any())).thenReturn(Collections.singletonList("permission"));
        final List<ConfigVerificationResult> verificationResults = ((VerifiableProcessor) processor).verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap());
        assertEquals(2, verificationResults.size());
        assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, verificationResults.get(1).getOutcome());

        runner.assertAllFlowFilesTransferred(PutBigQueryBatch.REL_SUCCESS);
    }

    @Test
    public void testFailedLoad() throws Exception {
        when(bq.writer(ArgumentMatchers.isA(WriteChannelConfiguration.class))).thenReturn(tableDataWriteChannel);
        when(tableDataWriteChannel.getJob()).thenReturn(job);
        when(job.waitFor(ArgumentMatchers.isA(RetryOption.class))).thenThrow(BigQueryException.class);

        final TestRunner runner = buildNewRunner(getProcessor());
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        runner.enqueue("{ \"data\": \"datavalue\" }");

        runner.run();

        runner.assertAllFlowFilesTransferred(PutBigQueryBatch.REL_FAILURE);
    }

    @Test
    public void testMandatoryProjectId() throws Exception {
        final TestRunner runner = buildNewRunner(getProcessor());
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        runner.removeProperty(PutBigQueryBatch.PROJECT_ID);
        runner.assertNotValid();
    }
}