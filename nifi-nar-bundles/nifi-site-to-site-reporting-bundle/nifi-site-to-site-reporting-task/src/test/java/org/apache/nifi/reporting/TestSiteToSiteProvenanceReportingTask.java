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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.provenance.ProvenanceEventBuilder;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

public class TestSiteToSiteProvenanceReportingTask {

    @Test
    public void testSerializedForm() throws IOException, InitializationException {
        final String uuid = "10000000-0000-0000-0000-000000000000";
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "xyz");
        attributes.put("xyz", "abc");
        attributes.put("filename", "file-" + uuid);

        final Map<String, String> prevAttrs = new HashMap<>();
        attributes.put("filename", "1234.xyz");

        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventTime(System.currentTimeMillis());
        builder.setEventType(ProvenanceEventType.RECEIVE);
        builder.setTransitUri("nifi://unit-test");
        attributes.put("uuid", uuid);
        builder.fromFlowFile(createFlowFile(3L, attributes));
        builder.setAttributes(prevAttrs, attributes);
        builder.setComponentId("1234");
        builder.setComponentType("dummy processor");
        final ProvenanceEventRecord event = builder.build();

        final List<byte[]> dataSent = new ArrayList<>();
        final SiteToSiteProvenanceReportingTask task = new SiteToSiteProvenanceReportingTask() {
            @SuppressWarnings("unchecked")
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
        };

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : task.getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }
        properties.put(SiteToSiteProvenanceReportingTask.BATCH_SIZE, "1000");

        final ReportingContext context = Mockito.mock(ReportingContext.class);
        Mockito.when(context.getStateManager())
                .thenReturn(new MockStateManager(task));
        Mockito.doAnswer(new Answer<PropertyValue>() {
            @Override
            public PropertyValue answer(final InvocationOnMock invocation) throws Throwable {
                final PropertyDescriptor descriptor = invocation.getArgumentAt(0, PropertyDescriptor.class);
                return new MockPropertyValue(properties.get(descriptor), null);
            }
        }).when(context).getProperty(Mockito.any(PropertyDescriptor.class));

        final long maxEventId = 2500;
        final AtomicInteger totalEvents = new AtomicInteger(0);

        final EventAccess eventAccess = Mockito.mock(EventAccess.class);
        Mockito.doAnswer(new Answer<List<ProvenanceEventRecord>>() {
            @Override
            public List<ProvenanceEventRecord> answer(final InvocationOnMock invocation) throws Throwable {
                final long startId = invocation.getArgumentAt(0, long.class);
                final int maxRecords = invocation.getArgumentAt(1, int.class);

                final List<ProvenanceEventRecord> eventsToReturn = new ArrayList<>();
                for (int i = (int) Math.max(0, startId); i < (int) (startId + maxRecords) && totalEvents.get() < maxEventId; i++) {
                    eventsToReturn.add(event);
                    totalEvents.getAndIncrement();
                }
                return eventsToReturn;
            }
        }).when(eventAccess).getProvenanceEvents(Mockito.anyLong(), Mockito.anyInt());

        final ProvenanceEventRepository provenanceRepository = Mockito.mock(ProvenanceEventRepository.class);
        Mockito.doAnswer(new Answer<Long>() {
            @Override
            public Long answer(final InvocationOnMock invocation) throws Throwable {
                return maxEventId;
            }
        }).when(provenanceRepository).getMaxEventId();

        Mockito.when(context.getEventAccess()).thenReturn(eventAccess);
        Mockito.when(eventAccess.getProvenanceRepository()).thenReturn(provenanceRepository);

        final ComponentLog logger = Mockito.mock(ComponentLog.class);
        final ReportingInitializationContext initContext = Mockito.mock(ReportingInitializationContext.class);
        Mockito.when(initContext.getIdentifier()).thenReturn(UUID.randomUUID().toString());
        Mockito.when(initContext.getLogger()).thenReturn(logger);


        task.initialize(initContext);
        task.onTrigger(context);

        assertEquals(3, dataSent.size());
        final String msg = new String(dataSent.get(0), StandardCharsets.UTF_8);
        JsonReader jsonReader = Json.createReader(new ByteArrayInputStream(msg.getBytes()));
        JsonObject msgArray = jsonReader.readArray().getJsonObject(0).getJsonObject("updatedAttributes");
        assertEquals(msgArray.getString("abc"), event.getAttributes().get("abc"));
    }

    public static FlowFile createFlowFile(final long id, final Map<String, String> attributes) {
        MockFlowFile mockFlowFile = new MockFlowFile(id);
        mockFlowFile.putAttributes(attributes);
        return mockFlowFile;
    }
}
