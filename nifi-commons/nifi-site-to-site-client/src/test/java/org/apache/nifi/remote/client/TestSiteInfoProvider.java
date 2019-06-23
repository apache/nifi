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
package org.apache.nifi.remote.client;

import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.protocol.http.HttpProxy;
import org.apache.nifi.remote.util.SiteToSiteRestApiClient;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestSiteInfoProvider {

    @Test
    public void testSecure() throws Exception {

        final Set<String> expectedClusterUrl = new LinkedHashSet<>(Arrays.asList(new String[]{"https://node1:8443", "https://node2:8443"}));
        final String expectedActiveClusterUrl = "https://node2:8443/nifi-api";
        final SSLContext expectedSslConText = mock(SSLContext.class);
        final HttpProxy expectedHttpProxy = mock(HttpProxy.class);

        final SiteInfoProvider siteInfoProvider = spy(new SiteInfoProvider());
        siteInfoProvider.setClusterUrls(expectedClusterUrl);
        siteInfoProvider.setSslContext(expectedSslConText);
        siteInfoProvider.setProxy(expectedHttpProxy);

        final ControllerDTO controllerDTO = new ControllerDTO();

        final PortDTO inputPort1 = new PortDTO();
        inputPort1.setName("input-one");
        inputPort1.setId("input-0001");

        final PortDTO inputPort2 = new PortDTO();
        inputPort2.setName("input-two");
        inputPort2.setId("input-0002");

        final PortDTO outputPort1 = new PortDTO();
        outputPort1.setName("output-one");
        outputPort1.setId("output-0001");

        final PortDTO outputPort2 = new PortDTO();
        outputPort2.setName("output-two");
        outputPort2.setId("output-0002");

        final Set<PortDTO> inputPorts = new HashSet<>();
        inputPorts.add(inputPort1);
        inputPorts.add(inputPort2);

        final Set<PortDTO> outputPorts = new HashSet<>();
        outputPorts.add(outputPort1);
        outputPorts.add(outputPort2);

        controllerDTO.setInputPorts(inputPorts);
        controllerDTO.setOutputPorts(outputPorts);
        controllerDTO.setRemoteSiteListeningPort(8081);
        controllerDTO.setRemoteSiteHttpListeningPort(8443);
        controllerDTO.setSiteToSiteSecure(true);

        // SiteInfoProvider uses SiteToSIteRestApiClient to get ControllerDTO.
        doAnswer(invocation -> {
            final SSLContext sslContext = invocation.getArgument(0);
            final HttpProxy httpProxy = invocation.getArgument(1);

            assertEquals(expectedSslConText, sslContext);
            assertEquals(expectedHttpProxy, httpProxy);

            final SiteToSiteRestApiClient apiClient = mock(SiteToSiteRestApiClient.class);

            when(apiClient.getController(eq(expectedClusterUrl))).thenReturn(controllerDTO);

            when(apiClient.getBaseUrl()).thenReturn(expectedActiveClusterUrl);

            return apiClient;
        }).when(siteInfoProvider).createSiteToSiteRestApiClient(any(), any());

        // siteInfoProvider should expose correct information of the remote NiFi cluster.
        assertEquals(controllerDTO.getRemoteSiteListeningPort(), siteInfoProvider.getSiteToSitePort());
        assertEquals(controllerDTO.getRemoteSiteHttpListeningPort(), siteInfoProvider.getSiteToSiteHttpPort());
        assertEquals(controllerDTO.isSiteToSiteSecure(), siteInfoProvider.isSecure());
        assertTrue(siteInfoProvider.isWebInterfaceSecure());

        assertEquals(inputPort1.getId(), siteInfoProvider.getInputPortIdentifier(inputPort1.getName()));
        assertEquals(inputPort2.getId(), siteInfoProvider.getInputPortIdentifier(inputPort2.getName()));
        assertEquals(outputPort1.getId(), siteInfoProvider.getOutputPortIdentifier(outputPort1.getName()));
        assertEquals(outputPort2.getId(), siteInfoProvider.getOutputPortIdentifier(outputPort2.getName()));
        assertNull(siteInfoProvider.getInputPortIdentifier("not-exist"));
        assertNull(siteInfoProvider.getOutputPortIdentifier("not-exist"));

        assertEquals(inputPort1.getId(), siteInfoProvider.getPortIdentifier(inputPort1.getName(), TransferDirection.SEND));
        assertEquals(outputPort1.getId(), siteInfoProvider.getPortIdentifier(outputPort1.getName(), TransferDirection.RECEIVE));

        assertEquals(expectedActiveClusterUrl, siteInfoProvider.getActiveClusterUrl().toString());

    }

    @Test
    public void testPlain() throws Exception {

        final Set<String> expectedClusterUrl = new LinkedHashSet<>(Arrays.asList(new String[]{"http://node1:8443, http://node2:8443"}));
        final String expectedActiveClusterUrl = "http://node2:8443/nifi-api";

        final SiteInfoProvider siteInfoProvider = spy(new SiteInfoProvider());
        siteInfoProvider.setClusterUrls(expectedClusterUrl);

        final ControllerDTO controllerDTO = new ControllerDTO();

        controllerDTO.setInputPorts(Collections.emptySet());
        controllerDTO.setOutputPorts(Collections.emptySet());
        controllerDTO.setRemoteSiteListeningPort(8081);
        controllerDTO.setRemoteSiteHttpListeningPort(8080);
        controllerDTO.setSiteToSiteSecure(false);

        // SiteInfoProvider uses SiteToSIteRestApiClient to get ControllerDTO.
        doAnswer(invocation -> {
            final SiteToSiteRestApiClient apiClient = mock(SiteToSiteRestApiClient.class);

            when(apiClient.getController(eq(expectedClusterUrl))).thenReturn(controllerDTO);

            when(apiClient.getBaseUrl()).thenReturn(expectedActiveClusterUrl);

            return apiClient;
        }).when(siteInfoProvider).createSiteToSiteRestApiClient(any(), any());

        // siteInfoProvider should expose correct information of the remote NiFi cluster.
        assertEquals(controllerDTO.getRemoteSiteListeningPort(), siteInfoProvider.getSiteToSitePort());
        assertEquals(controllerDTO.getRemoteSiteHttpListeningPort(), siteInfoProvider.getSiteToSiteHttpPort());
        assertEquals(controllerDTO.isSiteToSiteSecure(), siteInfoProvider.isSecure());
        assertFalse(siteInfoProvider.isWebInterfaceSecure());

        assertEquals(expectedActiveClusterUrl, siteInfoProvider.getActiveClusterUrl().toString());

    }

    @Test
    public void testConnectException() throws Exception {

        final Set<String> expectedClusterUrl = new LinkedHashSet<>(Arrays.asList(new String[]{"http://node1:8443, http://node2:8443"}));

        final SiteInfoProvider siteInfoProvider = spy(new SiteInfoProvider());
        siteInfoProvider.setClusterUrls(expectedClusterUrl);

        final ControllerDTO controllerDTO = new ControllerDTO();

        controllerDTO.setInputPorts(Collections.emptySet());
        controllerDTO.setOutputPorts(Collections.emptySet());
        controllerDTO.setRemoteSiteListeningPort(8081);
        controllerDTO.setRemoteSiteHttpListeningPort(8080);
        controllerDTO.setSiteToSiteSecure(false);

        // SiteInfoProvider uses SiteToSIteRestApiClient to get ControllerDTO.
        doAnswer(invocation -> {
            final SiteToSiteRestApiClient apiClient = mock(SiteToSiteRestApiClient.class);

            when(apiClient.getController(eq(expectedClusterUrl))).thenThrow(new IOException("Connection refused."));

            return apiClient;
        }).when(siteInfoProvider).createSiteToSiteRestApiClient(any(), any());

        try {
            siteInfoProvider.getSiteToSitePort();
            fail();
        } catch (IOException e) {
        }

        try {
            siteInfoProvider.getActiveClusterUrl();
            fail();
        } catch (IOException e) {
        }

    }

}
