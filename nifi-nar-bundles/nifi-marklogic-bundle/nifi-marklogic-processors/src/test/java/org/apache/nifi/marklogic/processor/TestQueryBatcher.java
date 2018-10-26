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

class TestQueryBatcher implements QueryBatcher {

    int batchSize = 100;
    int threadCount = 3;
    QueryDefinition queryDef;

    public TestQueryBatcher(QueryDefinition queryDef) {
        this.queryDef = queryDef;
    }

    @Override
    public String getJobName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getJobId() {
        // TODO Auto-generated method stub
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
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isStarted() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public QueryBatcher onUrisReady(QueryBatchListener listener) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public QueryBatcher onQueryFailure(QueryFailureListener listener) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public QueryBatcher onJobCompletion(QueryBatcherListener listener) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public void retry(QueryEvent queryEvent) {
        // TODO Auto-generated method stub
    }

    @Override
    public QueryBatchListener[] getQuerySuccessListeners() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryBatchListener[] getUrisReadyListeners() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryBatcherListener[] getQueryJobCompletionListeners() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryFailureListener[] getQueryFailureListeners() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setUrisReadyListeners(QueryBatchListener... listeners) {
        // TODO Auto-generated method stub
    }

    @Override
    public void setQueryFailureListeners(QueryFailureListener... listeners) {
        // TODO Auto-generated method stub
    }

    @Override
    public void setQueryJobCompletionListeners(QueryBatcherListener... listeners) {
        // TODO Auto-generated method stub
    }

    @Override
    public QueryBatcher withConsistentSnapshot() {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public QueryBatcher withForestConfig(ForestConfiguration forestConfig) {
        // TODO Auto-generated method stub
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
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isStopped() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public JobTicket getJobTicket() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void retryListener(QueryBatch batch, QueryBatchListener queryBatchListener) {
        // TODO Auto-generated method stub
    }

    @Override
    public void retryWithFailureListeners(QueryEvent queryEvent) {
        // TODO Auto-generated method stub
    }

    @Override
    public DatabaseClient getPrimaryClient() {
        // TODO Auto-generated method stub
        return null;
    }

    public QueryDefinition getQueryDefinition() {
        return queryDef;
    }
}