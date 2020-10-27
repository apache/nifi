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
package org.apache.nifi.toolkit.cli.impl.client;

import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.toolkit.cli.api.ClientFactory;
import org.apache.nifi.toolkit.cli.api.Command;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.api.Result;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.impl.JerseyNiFiClient;
import org.apache.nifi.toolkit.cli.impl.command.CommandProcessor;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.context.StandardContext;
import org.apache.nifi.toolkit.cli.impl.session.InMemorySession;
import org.glassfish.jersey.client.ClientProperties;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.client.Client;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class TestClientTimeout {

    private Context context;
    private ClientFactory<NiFiClient> nifiClientFactory;
    private ClientFactory<NiFiRegistryClient> nifiRegClientFactory;
    private final Client[] client = new Client[1];

    @Before
    public void setUp() throws Exception {
        nifiClientFactory = Mockito.spy(new NiFiClientFactory());
        Mockito.doAnswer(invocationOnMock -> {
            final JerseyNiFiClient nifiClient = Mockito.spy((JerseyNiFiClient) invocationOnMock.callRealMethod());
            Mockito.doNothing().when(nifiClient).close(); // avoid closing the client before getting the configuration in the test method
            client[0] = nifiClient.client;
            return nifiClient;
        }).when(nifiClientFactory).createClient(Mockito.any(Properties.class));

        nifiRegClientFactory = Mockito.mock(NiFiRegistryClientFactory.class);

        context = new StandardContext.Builder()
                .output(System.out)
                .session(new InMemorySession())
                .nifiClientFactory(nifiClientFactory)
                .nifiRegistryClientFactory(nifiRegClientFactory)
                .interactive(false)
                .build();
    }

    @After
    public void tearDown() {
        if (client[0] != null) {
            try {
                client[0].close();
            } catch (Throwable th) {
            }
            client[0] = null;
        }
    }

    @Test
    public void testNiFiClientTimeoutSettings() {
        testClientTimeoutSettings(new AbstractNiFiCommand<Result>("test", Result.class) {
            @Override
            public Result doExecute(NiFiClient client, Properties properties) {
                return null;
            }

            @Override
            public String getDescription() {
                return "";
            }
        });
    }

    private void testClientTimeoutSettings(Command<?> command) {
        command.initialize(context);
        final CommandProcessor processor = new CommandProcessor(Collections.singletonMap("test", command), Collections.emptyMap(), context);
        processor.process(new String[] { "test", "-cto", "1", "-rto", "2", "-baseUrl", "http://localhost:9999" });

        Assert.assertNotNull(client[0]);
        final Map<String, Object> clientProperties = client[0].getConfiguration().getProperties();
        Assert.assertEquals(1, clientProperties.get(ClientProperties.CONNECT_TIMEOUT));
        Assert.assertEquals(2, clientProperties.get(ClientProperties.READ_TIMEOUT));
    }

}
