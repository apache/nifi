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

package org.apache.nifi.reporting;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;

import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestSiteToSiteMetricsReportingTask {

    private ReportingContext context;
    private ProcessGroupStatus status;

    @Before
    public void setup() {
        status = new ProcessGroupStatus();
        status.setId("1234");
        status.setFlowFilesReceived(5);
        status.setBytesReceived(10000);
        status.setFlowFilesSent(10);
        status.setBytesSent(20000);
        status.setQueuedCount(100);
        status.setQueuedContentSize(1024L);
        status.setBytesRead(60000L);
        status.setBytesWritten(80000L);
        status.setActiveThreadCount(5);

        // create a processor status with processing time
        ProcessorStatus procStatus = new ProcessorStatus();
        procStatus.setProcessingNanos(123456789);

        Collection<ProcessorStatus> processorStatuses = new ArrayList<>();
        processorStatuses.add(procStatus);
        status.setProcessorStatus(processorStatuses);

        // create a group status with processing time
        ProcessGroupStatus groupStatus = new ProcessGroupStatus();
        groupStatus.setProcessorStatus(processorStatuses);

        Collection<ProcessGroupStatus> groupStatuses = new ArrayList<>();
        groupStatuses.add(groupStatus);
        status.setProcessGroupStatus(groupStatuses);
    }

    public MockSiteToSiteMetricsReportingTask initTask(Map<PropertyDescriptor, String> customProperties) throws InitializationException, IOException {

        final MockSiteToSiteMetricsReportingTask task = new MockSiteToSiteMetricsReportingTask();
        Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : task.getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }
        properties.putAll(customProperties);

        context = Mockito.mock(ReportingContext.class);
        Mockito.when(context.getStateManager()).thenReturn(new MockStateManager(task));
        Mockito.doAnswer(new Answer<PropertyValue>() {
            @Override
            public PropertyValue answer(final InvocationOnMock invocation) throws Throwable {
                final PropertyDescriptor descriptor = invocation.getArgumentAt(0, PropertyDescriptor.class);
                return new MockPropertyValue(properties.get(descriptor));
            }
        }).when(context).getProperty(Mockito.any(PropertyDescriptor.class));

        final EventAccess eventAccess = Mockito.mock(EventAccess.class);
        Mockito.when(context.getEventAccess()).thenReturn(eventAccess);
        Mockito.when(eventAccess.getControllerStatus()).thenReturn(status);

        final PropertyValue pValue = Mockito.mock(StandardPropertyValue.class);
        MockRecordWriter writer = new MockRecordWriter();
        Mockito.when(context.getProperty(MockSiteToSiteMetricsReportingTask.RECORD_WRITER)).thenReturn(pValue);
        Mockito.when(pValue.asControllerService(RecordSetWriterFactory.class)).thenReturn(writer);

        final ComponentLog logger = Mockito.mock(ComponentLog.class);
        final ReportingInitializationContext initContext = Mockito.mock(ReportingInitializationContext.class);
        Mockito.when(initContext.getIdentifier()).thenReturn(UUID.randomUUID().toString());
        Mockito.when(initContext.getLogger()).thenReturn(logger);
        task.initialize(initContext);

        return task;
    }

    @Test
    public void testValidationBothAmbariFormatRecordWriter() throws IOException {
        ValidationContext validationContext = Mockito.mock(ValidationContext.class);
        final String urlEL = "http://${hostname(true)}:8080/nifi";
        final String url = "http://localhost:8080/nifi";

        final MockSiteToSiteMetricsReportingTask task = new MockSiteToSiteMetricsReportingTask();
        Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : task.getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }

        properties.put(SiteToSiteMetricsReportingTask.FORMAT, SiteToSiteMetricsReportingTask.AMBARI_FORMAT.getValue());
        properties.put(SiteToSiteMetricsReportingTask.DESTINATION_URL, url);
        properties.put(SiteToSiteMetricsReportingTask.INSTANCE_URL, url);
        properties.put(SiteToSiteMetricsReportingTask.PORT_NAME, "port");

        final PropertyValue pValueUrl = Mockito.mock(StandardPropertyValue.class);
        Mockito.when(validationContext.newPropertyValue(url)).thenReturn(pValueUrl);
        Mockito.when(validationContext.newPropertyValue(urlEL)).thenReturn(pValueUrl);
        Mockito.when(pValueUrl.evaluateAttributeExpressions()).thenReturn(pValueUrl);
        Mockito.when(pValueUrl.getValue()).thenReturn(url);

        Mockito.doAnswer(new Answer<PropertyValue>() {
            @Override
            public PropertyValue answer(final InvocationOnMock invocation) throws Throwable {
                final PropertyDescriptor descriptor = invocation.getArgumentAt(0, PropertyDescriptor.class);
                return new MockPropertyValue(properties.get(descriptor));
            }
        }).when(validationContext).getProperty(Mockito.any(PropertyDescriptor.class));

        final PropertyValue pValue = Mockito.mock(StandardPropertyValue.class);
        Mockito.when(validationContext.getProperty(MockSiteToSiteMetricsReportingTask.RECORD_WRITER)).thenReturn(pValue);
        Mockito.when(pValue.isSet()).thenReturn(true);

        // should be invalid because both ambari format and record writer are set
        Collection<ValidationResult> list = task.validate(validationContext);
        Assert.assertEquals(1, list.size());
        Assert.assertEquals(SiteToSiteMetricsReportingTask.RECORD_WRITER.getDisplayName(), list.iterator().next().getInput());
    }

    @Test
    public void testValidationRecordFormatNoRecordWriter() throws IOException {
        ValidationContext validationContext = Mockito.mock(ValidationContext.class);
        final String urlEL = "http://${hostname(true)}:8080/nifi";
        final String url = "http://localhost:8080/nifi";

        final MockSiteToSiteMetricsReportingTask task = new MockSiteToSiteMetricsReportingTask();
        Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : task.getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }

        properties.put(SiteToSiteMetricsReportingTask.FORMAT, SiteToSiteMetricsReportingTask.RECORD_FORMAT.getValue());
        properties.put(SiteToSiteMetricsReportingTask.DESTINATION_URL, url);
        properties.put(SiteToSiteMetricsReportingTask.INSTANCE_URL, url);
        properties.put(SiteToSiteMetricsReportingTask.PORT_NAME, "port");

        final PropertyValue pValueUrl = Mockito.mock(StandardPropertyValue.class);
        Mockito.when(validationContext.newPropertyValue(url)).thenReturn(pValueUrl);
        Mockito.when(validationContext.newPropertyValue(urlEL)).thenReturn(pValueUrl);
        Mockito.when(pValueUrl.evaluateAttributeExpressions()).thenReturn(pValueUrl);
        Mockito.when(pValueUrl.getValue()).thenReturn(url);

        Mockito.doAnswer(new Answer<PropertyValue>() {
            @Override
            public PropertyValue answer(final InvocationOnMock invocation) throws Throwable {
                final PropertyDescriptor descriptor = invocation.getArgumentAt(0, PropertyDescriptor.class);
                return new MockPropertyValue(properties.get(descriptor));
            }
        }).when(validationContext).getProperty(Mockito.any(PropertyDescriptor.class));

        final PropertyValue pValue = Mockito.mock(StandardPropertyValue.class);
        Mockito.when(validationContext.getProperty(MockSiteToSiteMetricsReportingTask.RECORD_WRITER)).thenReturn(pValue);
        Mockito.when(pValue.isSet()).thenReturn(false);

        // should be invalid because both ambari format and record writer are set
        Collection<ValidationResult> list = task.validate(validationContext);
        Assert.assertEquals(1, list.size());
        Assert.assertEquals(SiteToSiteMetricsReportingTask.RECORD_WRITER.getDisplayName(), list.iterator().next().getInput());
    }

    @Test
    public void testAmbariFormat() throws IOException, InitializationException {

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(SiteToSiteMetricsReportingTask.FORMAT, SiteToSiteMetricsReportingTask.AMBARI_FORMAT.getValue());

        MockSiteToSiteMetricsReportingTask task = initTask(properties);
        task.onTrigger(context);

        assertEquals(1, task.dataSent.size());
        final String msg = new String(task.dataSent.get(0), StandardCharsets.UTF_8);
        JsonReader jsonReader = Json.createReader(new ByteArrayInputStream(msg.getBytes()));
        JsonArray array = jsonReader.readObject().getJsonArray("metrics");
        for(int i = 0; i < array.size(); i++) {
            JsonObject object = array.getJsonObject(i);
            assertEquals("nifi", object.getString("appid"));
            assertEquals("1234", object.getString("instanceid"));
            if(object.getString("metricname").equals("FlowFilesQueued")) {
                for(Entry<String, JsonValue> kv : object.getJsonObject("metrics").entrySet()) {
                    assertEquals("\"100\"", kv.getValue().toString());
                }
                return;
            }
        }
        fail();
    }

    @Test
    public void testRecordFormat() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(SiteToSiteMetricsReportingTask.FORMAT, SiteToSiteMetricsReportingTask.RECORD_FORMAT.getValue());
        properties.put(SiteToSiteMetricsReportingTask.RECORD_WRITER, "record-writer");
        MockSiteToSiteMetricsReportingTask task = initTask(properties);

        task.onTrigger(context);

        assertEquals(1, task.dataSent.size());
        String[] data = new String(task.dataSent.get(0)).split(",");
        assertEquals("\"nifi\"", data[0]);
        assertEquals("\"1234\"", data[1]);
        assertEquals("\"100\"", data[10]); // FlowFilesQueued
    }

    private static final class MockSiteToSiteMetricsReportingTask extends SiteToSiteMetricsReportingTask {

        public MockSiteToSiteMetricsReportingTask() throws IOException {
            super();
        }

        final List<byte[]> dataSent = new ArrayList<>();

        @Override
        protected SiteToSiteClient getClient() {
            final SiteToSiteClient client = Mockito.mock(SiteToSiteClient.class);
            final Transaction transaction = Mockito.mock(Transaction.class);

            try {
                Mockito.doAnswer(new Answer<Object>() {
                    @Override
                    public Object answer(final InvocationOnMock invocation) throws Throwable {
                        final byte[] data = invocation.getArgumentAt(0, byte[].class);
                        dataSent.add(data);
                        return null;
                    }
                }).when(transaction).send(Mockito.any(byte[].class), Mockito.any(Map.class));

                Mockito.when(client.createTransaction(Mockito.any(TransferDirection.class))).thenReturn(transaction);
            } catch (final Exception e) {
                e.printStackTrace();
                Assert.fail(e.toString());
            }

            return client;
        }
    }

}
