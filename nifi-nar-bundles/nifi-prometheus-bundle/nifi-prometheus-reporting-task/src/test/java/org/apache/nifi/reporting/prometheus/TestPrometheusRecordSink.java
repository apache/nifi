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
package org.apache.nifi.reporting.prometheus;


import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.prometheus.util.PrometheusMetricsUtil;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.ListRecordSet;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockControllerServiceInitializationContext;
import org.apache.nifi.util.MockPropertyValue;
import org.apache.nifi.util.MockVariableRegistry;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPrometheusRecordSink {

    private static final String portString = "7077";

    @Test
    public void testSendData() throws IOException, InitializationException {
        PrometheusRecordSink sink = initTask();

        List<RecordField> recordFields = Arrays.asList(
                new RecordField("field1", RecordFieldType.INT.getDataType()),
                new RecordField("field2", RecordFieldType.DECIMAL.getDecimalDataType(30, 10)),
                new RecordField("field3", RecordFieldType.STRING.getDataType())
        );
        RecordSchema recordSchema = new SimpleRecordSchema(recordFields);

        Map<String, Object> row1 = new HashMap<>();
        row1.put("field1", 15);
        row1.put("field2", BigDecimal.valueOf(12.34567D));
        row1.put("field3", "Hello");

        Map<String, Object> row2 = new HashMap<>();
        row2.put("field1", 6);
        row2.put("field2", BigDecimal.valueOf(0.1234567890123456789D));
        row2.put("field3", "World!");

        RecordSet recordSet = new ListRecordSet(recordSchema, Arrays.asList(
                new MapRecord(recordSchema, row1),
                new MapRecord(recordSchema, row2)
        ));

        Map<String, String> attributes = new HashMap<>();
        attributes.put("a", "Hello");
        WriteResult writeResult = sink.sendData(recordSet, attributes, true);
        assertNotNull(writeResult);
        assertEquals(2, writeResult.getRecordCount());
        assertEquals("Hello", writeResult.getAttributes().get("a"));


        final String content = getMetrics();
        assertTrue(content.contains("field1{field3=\"Hello\",} 15.0\nfield1{field3=\"World!\",} 6.0\n"));
        assertTrue(content.contains("field2{field3=\"Hello\",} 12.34567\nfield2{field3=\"World!\",} 0.12345678901234568\n"));

        try {
            sink.onStopped();
        } catch (Exception e) {
            // Do nothing, just need to shut down the server before the next run
        }
    }

    @Test
    public void testTwoInstances() throws InitializationException {
        PrometheusRecordSink sink1 = initTask();
        try {
            PrometheusRecordSink sink2 = initTask();
            fail("Should have reported Address In Use");
        } catch (ProcessException pe) {
            // Do nothing, this is the expected behavior
        }
        try {
            sink1.onStopped();
        } catch (Exception e) {
            // Do nothing, just need to shut down the server before the next run
        }
    }

    private String getMetrics() throws IOException {
        URL url = new URL("http://localhost:" + portString + "/metrics");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        int status = con.getResponseCode();
        Assert.assertEquals(HttpURLConnection.HTTP_OK, status);

        HttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet("http://localhost:" + portString + "/metrics");
        HttpResponse response = client.execute(request);
        HttpEntity entity = response.getEntity();
        return EntityUtils.toString(entity);
    }

    private PrometheusRecordSink initTask() throws InitializationException {

        final ComponentLog logger = mock(ComponentLog.class);
        final PrometheusRecordSink task = new PrometheusRecordSink();
        ConfigurationContext context = mock(ConfigurationContext.class);
        final StateManager stateManager = new MockStateManager(task);
        final MockVariableRegistry variableRegistry = new MockVariableRegistry();
        final PropertyValue pValue = mock(StandardPropertyValue.class);

        variableRegistry.setVariable(new VariableDescriptor("port"), portString);
        when(context.getProperty(PrometheusMetricsUtil.METRICS_ENDPOINT_PORT)).thenReturn(new MockPropertyValue("${port}", null, variableRegistry));
        when(context.getProperty(PrometheusRecordSink.SSL_CONTEXT)).thenReturn(pValue);
        when(pValue.asControllerService(SSLContextService.class)).thenReturn(null);

        final ControllerServiceInitializationContext initContext = new MockControllerServiceInitializationContext(task, UUID.randomUUID().toString(), logger, stateManager);
        task.initialize(initContext);
        task.onScheduled(context);

        return task;
    }
}