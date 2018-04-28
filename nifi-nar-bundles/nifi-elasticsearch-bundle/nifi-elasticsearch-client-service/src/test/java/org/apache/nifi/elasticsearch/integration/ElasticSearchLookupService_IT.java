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

package org.apache.nifi.elasticsearch.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.elasticsearch.ElasticSearchClientService;
import org.apache.nifi.elasticsearch.ElasticSearchClientServiceImpl;
import org.apache.nifi.elasticsearch.ElasticSearchLookupService;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ElasticSearchLookupService_IT {
    private TestRunner runner;
    private ElasticSearchClientService service;
    private ElasticSearchLookupService lookupService;

    static final Map<String, Object> QUERY;

    static {
        QUERY = new HashMap<String, Object>(){{
            put("size", 1000);
            put("query", new HashMap<String, Object>() {{
                put("match", new HashMap<String, String>(){{
                    put("msg", "five");
                }});
            }});
            put("aggs", new HashMap<>());
        }};
    }

    @Before
    public void before() throws Exception {
        runner = TestRunners.newTestRunner(TestControllerServiceProcessor.class);
        service = new ElasticSearchClientServiceImpl();
        lookupService = new ElasticSearchLookupService();
        runner.addControllerService("Client Service", service);
        runner.addControllerService("Lookup Service", lookupService);
        runner.setProperty(service, ElasticSearchClientService.HTTP_HOSTS, "http://localhost:9400");
        runner.setProperty(service, ElasticSearchClientService.CONNECT_TIMEOUT, "10000");
        runner.setProperty(service, ElasticSearchClientService.SOCKET_TIMEOUT, "60000");
        runner.setProperty(service, ElasticSearchClientService.RETRY_TIMEOUT, "60000");
        runner.setProperty(TestControllerServiceProcessor.CLIENT_SERVICE, "Client Service");
        runner.setProperty(TestControllerServiceProcessor.LOOKUP_SERVICE, "Lookup Service");
        runner.setProperty(lookupService, ElasticSearchLookupService.CLIENT_SERVICE, "Client Service");
        runner.setProperty(lookupService, ElasticSearchLookupService.INDEX, "messages");
        runner.setProperty(lookupService, ElasticSearchLookupService.TYPE, "message");

        try {
            runner.enableControllerService(service);
            runner.enableControllerService(lookupService);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
    public void testValidity() throws Exception {
        setDefaultSchema();
        runner.assertValid();
    }

    @Test
    public void testLookupInvalidId() throws Exception {
        final Map<String, Object> badId = new HashMap<String, Object>() {{
            put("_id", 100);
        }};
        final Map<String, Object> badQuery = new HashMap<String, Object>(){{
            put("query", QUERY);
        }};

        Map<Map, String> pairs = new HashMap<>();
        pairs.put(badId, "_id was supplied, but it was not a String.");
        pairs.put(badQuery, "query was supplied, but it was not a String.");
        pairs.put(new HashMap<String, Object>() {{
            put("_id", "100");
            put("query", new ObjectMapper().writeValueAsString(QUERY));
        }}, "\"_id\" and \"query\" cannot be used at the same time as keys.");
        pairs.put(new HashMap<String, Object>(), "Either \"_id\" or \"query\" must be supplied as keys to lookup(Map)");

        for (Map.Entry<Map, String> entry : pairs.entrySet()) {
            String message = null;
            try {
                lookupService.lookup(entry.getKey());
            } catch (Exception ex) {
                message = ex.getMessage();
            } finally {
                Assert.assertNotNull(message);
                Assert.assertEquals(entry.getValue(), message);
            }
        }
    }

    @Test
    public void testIdNotFound() throws Exception {
        final String _id = "100";
        Map<String, Object> coordinates = new HashMap<String, Object>() {{
            put("_id", _id);
        }};

        Optional<Record> response = lookupService.lookup(coordinates);

        Assert.assertNotNull(response);
        Assert.assertFalse(response.isPresent());
    }

    @Test
    public void testQueryNoResults() throws Exception {
        Map<String, Object> coordinates = new HashMap<String, Object>() {{
            put("query", new ObjectMapper().writeValueAsString(new HashMap<String, Object>() {{
                put("size", 1000);
                put("query", new HashMap<String, Object>() {{
                    put("match", new HashMap<String, String>() {{
                        put("msg", "six");
                    }});
                }});
                put("aggs", new HashMap<>());
            }}));
        }};

        Optional response = lookupService.lookup(coordinates);

        Assert.assertNotNull(response);
        Assert.assertFalse(response.isPresent());
    }

    @Test
    public void testInvalidQueryErrorHandling() {
        Exception error = null;
        try {
            //Lookup using an invalid ES query
            lookupService.lookup(new HashMap<String, Object>(){{
                put("_id", new HashMap<String, Object>(){{
                    put("size", 1000);
                    put("query", new HashMap<String, Object>() {{
                        put("mismatch", new HashMap<String, String>(){{
                            put("msg", "six");
                        }});
                    }});
                    put("aggs", new HashMap<>());
                }});
            }});
        } catch (Exception ex) {
            error = ex;
        } finally {
            Assert.assertNotNull(error);
            Assert.assertTrue(error instanceof LookupFailureException);
        }
    }

    private void setDefaultSchema() throws Exception {
        runner.disableControllerService(lookupService);
        SchemaRegistry registry = new TestSchemaRegistry();
        runner.addControllerService("registry", registry);
        runner.setProperty(lookupService, SchemaAccessUtils.SCHEMA_REGISTRY, "registry");
        runner.enableControllerService(registry);
        runner.enableControllerService(lookupService);
    }


    @Test
    public void testLookupById() throws Exception {
        setDefaultSchema();
        final String _id = "10";
        Map<String, Object> coordinates = new HashMap<String, Object>(){{
            put("_id", _id);

        }};

        Optional<Record> response = lookupService.lookup(coordinates);

        Assert.assertNotNull(response);
        Record rec = response.get();
        Assert.assertNotNull(rec);
        Assert.assertNotNull(rec.toString(), rec.getAsString("msg"));
        Assert.assertEquals(rec.getAsString("msg"), "four");
    }

    @Test
    public void testLookupByQuery() throws Exception {
        setDefaultSchema();

        final ObjectMapper mapper = new ObjectMapper();

        Map<String, Object> coordinates = new HashMap<String, Object>(){{
            put("query", mapper.writeValueAsString(QUERY));
        }};

        Optional<Record> response = lookupService.lookup(coordinates);

        Assert.assertNotNull(response);
        Record rec = response.get();
        Assert.assertNotNull(rec);
        Assert.assertNotNull(rec.toString(), rec.getAsString("msg"));
        Assert.assertEquals(rec.getAsString("msg"), "five");
    }

    @Test
    public void testDetectedSchema() throws LookupFailureException {
        runner.disableControllerService(lookupService);
        runner.setProperty(lookupService, ElasticSearchLookupService.INDEX, "complex");
        runner.setProperty(lookupService, ElasticSearchLookupService.TYPE, "complex");
        runner.enableControllerService(lookupService);
        Map<String, Object> coordinates = new HashMap<String, Object>(){{
            put("_id", "1");
        }};

        Optional<Record> response = lookupService.lookup(coordinates);
        Assert.assertNotNull(response);
        Record rec = response.get();
        RecordSchema schema = rec.getSchema();
        RecordSchema subSchema = ((RecordDataType)schema.getField("subField").get().getDataType()).getChildSchema();
        Record subRec = rec.getAsRecord("subField", subSchema);
        Assert.assertNotNull(rec);
        Assert.assertNotNull(subRec);
        Assert.assertEquals("Hello, world", rec.getValue("msg"));
        Assert.assertNotNull(rec.getValue("subField"));
        Assert.assertEquals(new Long(100000), subRec.getValue("longField"));
        Assert.assertEquals("2018-04-10T12:18:05Z", subRec.getValue("dateField"));
    }
}
