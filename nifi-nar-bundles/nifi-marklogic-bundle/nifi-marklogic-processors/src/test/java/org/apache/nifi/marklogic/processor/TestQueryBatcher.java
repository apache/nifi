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
package org.apache.nifi.marklogic.processor;

import java.util.concurrent.TimeUnit;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.ForestConfiguration;
import com.marklogic.client.datamovement.JobTicket;
import com.marklogic.client.datamovement.QueryBatch;
import com.marklogic.client.datamovement.QueryBatchListener;
import com.marklogic.client.datamovement.QueryBatcher;
import com.marklogic.client.datamovement.QueryBatcherListener;
import com.marklogic.client.datamovement.QueryEvent;
import com.marklogic.client.datamovement.QueryFailureListener;
import com.marklogic.client.query.QueryDefinition;

/*
 * Methods of QueryBatcher interface are minimally implemented for the purpose of testing.
 */
class TestQueryBatcher implements QueryBatcher {

    int batchSize = 100;
    int threadCount = 3;
    QueryDefinition queryDef;

    public TestQueryBatcher(QueryDefinition queryDef) {
        this.queryDef = queryDef;
    }

    @Override
    public String getJobName() {
        return null;
    }

    @Override
    public String getJobId() {
        return null;
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public int getThreadCount() {
        return threadCount;
    }

    @Override
    public ForestConfiguration getForestConfig() {
        return null;
    }

    @Override
    public boolean isStarted() {
        return false;
    }

    @Override
    public QueryBatcher onUrisReady(QueryBatchListener listener) {
        return this;
    }

    @Override
    public QueryBatcher onQueryFailure(QueryFailureListener listener) {
        return this;
    }

    @Override
    public QueryBatcher onJobCompletion(QueryBatcherListener listener) {
        return this;
    }

    @Override
    public void retry(QueryEvent queryEvent) {
    }

    @Override
    public QueryBatchListener[] getQuerySuccessListeners() {
        return null;
    }

    @Override
    public QueryBatchListener[] getUrisReadyListeners() {
        return null;
    }

    @Override
    public QueryBatcherListener[] getQueryJobCompletionListeners() {
        return null;
    }

    @Override
    public QueryFailureListener[] getQueryFailureListeners() {
        return null;
    }

    @Override
    public void setUrisReadyListeners(QueryBatchListener... listeners) {
    }

    @Override
    public void setQueryFailureListeners(QueryFailureListener... listeners) {
    }

    @Override
    public void setQueryJobCompletionListeners(QueryBatcherListener... listeners) {
    }

    @Override
    public QueryBatcher withConsistentSnapshot() {
        return this;
    }

    @Override
    public QueryBatcher withForestConfig(ForestConfiguration forestConfig) {
        return this;
    }

    @Override
    public QueryBatcher withJobName(String jobName) {
        return this;
    }

    @Override
    public QueryBatcher withJobId(String jobId) {
        return this;
    }

    @Override
    public QueryBatcher withBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    @Override
    public QueryBatcher withThreadCount(int threadCount) {
        this.threadCount = threadCount;
        return this;
    }

    @Override
    public boolean awaitCompletion() {
        return false;
    }

    @Override
    public boolean awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public boolean isStopped() {
        return false;
    }

    @Override
    public JobTicket getJobTicket() {
        return null;
    }

    @Override
    public void retryListener(QueryBatch batch, QueryBatchListener queryBatchListener) {
    }

    @Override
    public void retryWithFailureListeners(QueryEvent queryEvent) {
    }

    @Override
    public DatabaseClient getPrimaryClient() {
        return null;
    }

    public QueryDefinition getQueryDefinition() {
        return queryDef;
    }
}