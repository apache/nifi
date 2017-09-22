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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestSiteToSiteBulletinReportingTask {

    @Test
    public void testSerializedForm() throws IOException, InitializationException {
        // creating the list of bulletins
        final List<Bulletin> bulletins = new ArrayList<Bulletin>();
        bulletins.add(BulletinFactory.createBulletin("group-id", "group-name", "source-id", "source-name", "category", "severity", "message"));

        // mock the access to the list of bulletins
        final ReportingContext context = Mockito.mock(ReportingContext.class);
        final BulletinRepository repository = Mockito.mock(BulletinRepository.class);
        Mockito.when(context.getBulletinRepository()).thenReturn(repository);
        Mockito.when(repository.findBulletins(Mockito.any(BulletinQuery.class))).thenReturn(bulletins);

        // creating reporting task
        final MockSiteToSiteBulletinReportingTask task = new MockSiteToSiteBulletinReportingTask();
        Mockito.when(context.getStateManager()).thenReturn(new MockStateManager(task));

        // settings properties and mocking access to properties
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : task.getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }
        properties.put(SiteToSiteBulletinReportingTask.BATCH_SIZE, "1000");
        properties.put(SiteToSiteBulletinReportingTask.PLATFORM, "nifi");

        Mockito.doAnswer(new Answer<PropertyValue>() {
            @Override
            public PropertyValue answer(final InvocationOnMock invocation) throws Throwable {
                final PropertyDescriptor descriptor = invocation.getArgumentAt(0, PropertyDescriptor.class);
                return new MockPropertyValue(properties.get(descriptor));
            }
        }).when(context).getProperty(Mockito.any(PropertyDescriptor.class));

        // setup the mock initialization context
        final ComponentLog logger = Mockito.mock(ComponentLog.class);
        final ReportingInitializationContext initContext = Mockito.mock(ReportingInitializationContext.class);
        Mockito.when(initContext.getIdentifier()).thenReturn(UUID.randomUUID().toString());
        Mockito.when(initContext.getLogger()).thenReturn(logger);

        task.initialize(initContext);
        task.onTrigger(context);

        // test checking
        assertEquals(1, task.dataSent.size());
        final String msg = new String(task.dataSent.get(0), StandardCharsets.UTF_8);
        JsonReader jsonReader = Json.createReader(new ByteArrayInputStream(msg.getBytes()));
        JsonObject bulletinJson = jsonReader.readArray().getJsonObject(0);
        assertEquals("message", bulletinJson.getString("bulletinMessage"));
        assertEquals("group-name", bulletinJson.getString("bulletinGroupName"));
    }

    @Test
    public void testWhenProvenanceMaxIdEqualToLastEventIdInStateManager() throws IOException, InitializationException {
        // creating the list of bulletins
        final List<Bulletin> bulletins = new ArrayList<Bulletin>();
        bulletins.add(BulletinFactory.createBulletin("category", "severity", "message"));
        bulletins.add(BulletinFactory.createBulletin("category", "severity", "message"));
        bulletins.add(BulletinFactory.createBulletin("category", "severity", "message"));
        bulletins.add(BulletinFactory.createBulletin("category", "severity", "message"));

        // mock the access to the list of bulletins
        final ReportingContext context = Mockito.mock(ReportingContext.class);
        final BulletinRepository repository = Mockito.mock(BulletinRepository.class);
        Mockito.when(context.getBulletinRepository()).thenReturn(repository);
        Mockito.when(repository.findBulletins(Mockito.any(BulletinQuery.class))).thenReturn(bulletins);

        final long maxEventId = getMaxBulletinId(bulletins);;

        // create the mock reporting task and mock state manager
        final MockSiteToSiteBulletinReportingTask task = new MockSiteToSiteBulletinReportingTask();
        final MockStateManager stateManager = new MockStateManager(task);

        // settings properties and mocking access to properties
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : task.getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }
        properties.put(SiteToSiteBulletinReportingTask.BATCH_SIZE, "1000");
        properties.put(SiteToSiteBulletinReportingTask.PLATFORM, "nifi");
        properties.put(SiteToSiteBulletinReportingTask.TRANSPORT_PROTOCOL, SiteToSiteTransportProtocol.HTTP.name());
        properties.put(SiteToSiteBulletinReportingTask.HTTP_PROXY_HOSTNAME, "localhost");
        properties.put(SiteToSiteBulletinReportingTask.HTTP_PROXY_PORT, "80");
        properties.put(SiteToSiteBulletinReportingTask.HTTP_PROXY_USERNAME, "username");
        properties.put(SiteToSiteBulletinReportingTask.HTTP_PROXY_PASSWORD, "password");

        Mockito.doAnswer(new Answer<PropertyValue>() {
            @Override
            public PropertyValue answer(final InvocationOnMock invocation) throws Throwable {
                final PropertyDescriptor descriptor = invocation.getArgumentAt(0, PropertyDescriptor.class);
                return new MockPropertyValue(properties.get(descriptor));
            }
        }).when(context).getProperty(Mockito.any(PropertyDescriptor.class));

        // create the state map and set the last id to the same value as maxEventId
        final Map<String,String> state = new HashMap<>();
        state.put(SiteToSiteProvenanceReportingTask.LAST_EVENT_ID_KEY, String.valueOf(maxEventId));
        stateManager.setState(state, Scope.LOCAL);

        // setup the mock reporting context to return the mock state manager
        Mockito.when(context.getStateManager()).thenReturn(stateManager);

        // setup the mock initialization context
        final ComponentLog logger = Mockito.mock(ComponentLog.class);
        final ReportingInitializationContext initContext = Mockito.mock(ReportingInitializationContext.class);
        Mockito.when(initContext.getIdentifier()).thenReturn(UUID.randomUUID().toString());
        Mockito.when(initContext.getLogger()).thenReturn(logger);

        task.initialize(initContext);

        // execute the reporting task and should not produce any data b/c max id same as previous id
        task.onTrigger(context);
        assertEquals(0, task.dataSent.size());
    }

    private static final class MockSiteToSiteBulletinReportingTask extends SiteToSiteBulletinReportingTask {

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
                }).when(transaction).send(Mockito.any(byte[].class), Mockito.anyMapOf(String.class, String.class));

                Mockito.when(client.createTransaction(Mockito.any(TransferDirection.class))).thenReturn(transaction);
            } catch (final Exception e) {
                e.printStackTrace();
                Assert.fail(e.toString());
            }

            return client;
        }

    }

    private Long getMaxBulletinId(List<Bulletin> bulletins) {
        long result = -1L;
        for (Bulletin bulletin : bulletins) {
            if(bulletin.getId() > result) {
                result = bulletin.getId();
            }
        }
        return result;
    }

}
