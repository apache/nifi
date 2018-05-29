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
package com.marklogic.nifi.processor;

import com.marklogic.client.ext.datamovement.job.SimpleQueryBatcherJob;
import org.apache.nifi.processor.ProcessContext;
import org.junit.Before;
import org.junit.Test;

public class QueryMarkLogicTest extends AbstractMarkLogicProcessorTest {

    private TestQueryMarkLogic processor;

    @Before
    public void setup() {
        processor = new TestQueryMarkLogic();
        initialize(processor);

        processContext.setProperty(QueryMarkLogic.CONSISTENT_SNAPSHOT, "true");
    }

    @Test
    public void queryCollections() {
        processContext.setProperty(QueryMarkLogic.COLLECTIONS, "coll1,coll2");

        processor.onTrigger(processContext, mockProcessSessionFactory);

        String[] collections = processor.job.getWhereCollections();
        assertEquals(2, collections.length);
        assertEquals("coll1", collections[0]);
        assertEquals("coll2", collections[1]);
    }

    @Test
    public void queryJobProperties() {
        processContext.setProperty(QueryMarkLogic.BATCH_SIZE, "35");
        processContext.setProperty(QueryMarkLogic.THREAD_COUNT, "15");
    /*  processContext.setProperty(QueryMarkLogic.URI_PATTERN,".*nifi.*");
        processContext.setProperty(QueryMarkLogic.URIS_QUERY,"<cts:and-query/>"); */

        processor.onTrigger(processContext, mockProcessSessionFactory);

        assertEquals(1, processor.relationships.size());
        int jobBatchSize = processor.job.getBatchSize();
        int jobThreadCount = processor.job.getThreadCount();
    /*  String uriPattern = processor.job.getWhereUriPattern();
        String uriQuery = processor.job.getWhereUrisQuery();
        assertEquals(".*nifi.*", uriPattern);
        assertEquals("<cts:and-query/>", uriQuery); */
        assertEquals(35, jobBatchSize);
        assertEquals(15, jobThreadCount);
    }

}

class TestQueryMarkLogic extends QueryMarkLogic {

    SimpleQueryBatcherJob job;

    @Override
    protected void runJob(SimpleQueryBatcherJob job, ProcessContext context) {
        this.job = job;
    }

}