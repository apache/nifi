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

import java.util.Iterator;

import com.marklogic.client.DatabaseClient.ConnectionType;
import com.marklogic.client.datamovement.Batcher;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.ForestConfiguration;
import com.marklogic.client.datamovement.JobReport;
import com.marklogic.client.datamovement.JobTicket;
import com.marklogic.client.datamovement.QueryBatcher;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.query.RawCombinedQueryDefinition;
import com.marklogic.client.query.RawStructuredQueryDefinition;
import com.marklogic.client.query.StringQueryDefinition;
import com.marklogic.client.query.StructuredQueryDefinition;

class TestDataMovementManager implements DataMovementManager {
    QueryDefinition queryDef = null;
    @Override
    public void release() {
        // TODO Auto-generated method stub
    }

    @Override
    public JobTicket startJob(WriteBatcher batcher) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public JobTicket startJob(QueryBatcher batcher) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public JobReport getJobReport(JobTicket ticket) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public JobTicket getActiveJob(String jobId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void stopJob(JobTicket ticket) {
        // TODO Auto-generated method stub
    }

    @Override
    public void stopJob(Batcher batcher) {
        // TODO Auto-generated method stub
    }

    @Override
    public WriteBatcher newWriteBatcher() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryBatcher newQueryBatcher(StructuredQueryDefinition query) {
        queryDef = query;
        return new TestQueryBatcher(query);
    }

    @Override
    public QueryBatcher newQueryBatcher(RawStructuredQueryDefinition query) {
        queryDef = query;
        return new TestQueryBatcher(query);
    }

    @Override
    public QueryBatcher newQueryBatcher(StringQueryDefinition query) {
        return new TestQueryBatcher(query);
    }

    @Override
    public QueryBatcher newQueryBatcher(RawCombinedQueryDefinition query) {
        return new TestQueryBatcher(query);
    }

    @Override
    public QueryBatcher newQueryBatcher(Iterator<String> iterator) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ForestConfiguration readForestConfig() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ConnectionType getConnectionType() {
        // TODO Auto-generated method stub
        return null;
    }
}