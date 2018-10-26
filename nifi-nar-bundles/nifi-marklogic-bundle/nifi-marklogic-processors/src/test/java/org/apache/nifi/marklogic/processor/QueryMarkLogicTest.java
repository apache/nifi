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

import org.apache.nifi.reporting.InitializationException;
import org.junit.Before;
import org.junit.Test;

import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.RawCombinedQueryDefinition;
import com.marklogic.client.query.RawStructuredQueryDefinition;
import com.marklogic.client.query.StructuredQueryDefinition;

public class QueryMarkLogicTest extends AbstractMarkLogicProcessorTest {

    private TestQueryMarkLogic processor;

    @Before
    public void setup() throws InitializationException {
        processor = new TestQueryMarkLogic();
        initialize(processor);
        processContext.setProperty(TestQueryMarkLogic.THREAD_COUNT.getName(), "5");
        processContext.setProperty(TestQueryMarkLogic.BATCH_SIZE, "15");
        processContext.setProperty(TestQueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.COLLECTION.name());
        processContext.setProperty(TestQueryMarkLogic.QUERY, "test");
        processContext.setProperty(TestQueryMarkLogic.DATABASE_CLIENT_SERVICE, databaseClientServiceIdentifier);
    }

    @Test
    public void testCollectionsQueryMarLogic() throws Exception {
        runner.enableControllerService(service);
        runner.assertValid(service);
        processor.initialize(initializationContext);
        processor.onTrigger(processContext, mockProcessSessionFactory);
        TestQueryBatcher queryBatcher = (TestQueryBatcher) processor.getQueryBatcher();
        assertEquals(15, queryBatcher.getBatchSize());
        assertEquals(5, queryBatcher.getThreadCount());
        assertTrue(queryBatcher.getQueryDefinition() instanceof StructuredQueryDefinition);
    }
    
    @Test
    public void testCombinedJsonQueryMarLogic() throws Exception {
        StringHandle handle;
        TestQueryBatcher queryBatcher;
        
        processContext.setProperty(TestQueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.COMBINED_JSON.name());
        processor.initialize(initializationContext);
        processor.onTrigger(processContext, mockProcessSessionFactory);
        queryBatcher = (TestQueryBatcher) processor.getQueryBatcher();
        assertTrue(queryBatcher.getQueryDefinition() instanceof RawCombinedQueryDefinition);
        handle = (StringHandle) ((RawCombinedQueryDefinition)queryBatcher.getQueryDefinition()).getHandle();
        assertEquals(handle.getFormat(), Format.JSON);
    }

    @Test
    public void testCombinedXmlQueryMarLogic() throws Exception {
        StringHandle handle;
        TestQueryBatcher queryBatcher;

        processContext.setProperty(TestQueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.COMBINED_XML.name());
        processor.initialize(initializationContext);
        processor.onTrigger(processContext, mockProcessSessionFactory);
        queryBatcher = (TestQueryBatcher) processor.getQueryBatcher();
        assertTrue(queryBatcher.getQueryDefinition() instanceof RawCombinedQueryDefinition);
        handle = (StringHandle) ((RawCombinedQueryDefinition)queryBatcher.getQueryDefinition()).getHandle();
        assertEquals(handle.getFormat(), Format.XML);
    }
    @Test

    public void testStructuredJsonQueryMarLogic() throws Exception {
        StringHandle handle;
        TestQueryBatcher queryBatcher;

        processContext.setProperty(TestQueryMarkLogic.CONSISTENT_SNAPSHOT, "false");

        processContext.setProperty(TestQueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.STRUCTURED_JSON.name());
        processor.initialize(initializationContext);
        processor.onTrigger(processContext, mockProcessSessionFactory);
        queryBatcher = (TestQueryBatcher) processor.getQueryBatcher();
        assertTrue(queryBatcher.getQueryDefinition() instanceof RawStructuredQueryDefinition);
        handle = (StringHandle) ((RawStructuredQueryDefinition)queryBatcher.getQueryDefinition()).getHandle();
        assertEquals(handle.getFormat(), Format.JSON);
    }

    @Test
    public void testStructuredXmlQueryMarLogic() throws Exception {
        StringHandle handle;
        TestQueryBatcher queryBatcher;

        processContext.setProperty(TestQueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.STRUCTURED_XML.name());
        processor.initialize(initializationContext);
        processor.onTrigger(processContext, mockProcessSessionFactory);
        queryBatcher = (TestQueryBatcher) processor.getQueryBatcher();
        assertTrue(queryBatcher.getQueryDefinition() instanceof RawStructuredQueryDefinition);
        handle = (StringHandle) ((RawStructuredQueryDefinition)queryBatcher.getQueryDefinition()).getHandle();
        assertEquals(handle.getFormat(), Format.XML);
    }
}
