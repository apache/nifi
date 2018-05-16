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
package org.apache.nifi.remote;

import org.apache.nifi.remote.cluster.NodeInformation;
import org.apache.nifi.remote.protocol.RequestType;
import org.apache.nifi.remote.protocol.ServerProtocol;
import org.apache.nifi.util.NiFiProperties;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestSocketRemoteSiteListener {

    @BeforeClass
    public static void setup() {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, "src/test/resources/nifi.properties");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.remote", "DEBUG");
    }

    @Test
    public void testRequestPeerList() throws Exception {
        Method method = SocketRemoteSiteListener.class.getDeclaredMethod("handleRequest",
                ServerProtocol.class, Peer.class, RequestType.class);
        method.setAccessible(true);

        final NiFiProperties nifiProperties = spy(NiFiProperties.class);
        final int apiPort = 8080;
        final int remoteSocketPort = 8081;
        final String remoteInputHost = "node1.example.com";
        when(nifiProperties.getPort()).thenReturn(apiPort);
        when(nifiProperties.getRemoteInputHost()).thenReturn(remoteInputHost);
        when(nifiProperties.getRemoteInputPort()).thenReturn(remoteSocketPort);
        when(nifiProperties.getRemoteInputHttpPort()).thenReturn(null); // Even if HTTP transport is disabled, RAW should work.
        when(nifiProperties.isSiteToSiteHttpEnabled()).thenReturn(false);
        when(nifiProperties.isSiteToSiteSecure()).thenReturn(false);
        final SocketRemoteSiteListener listener = new SocketRemoteSiteListener(remoteSocketPort, null, nifiProperties);

        final ServerProtocol serverProtocol = mock(ServerProtocol.class);
        doAnswer(invocation -> {
            final NodeInformation self = invocation.getArgumentAt(2, NodeInformation.class);
            // Listener should inform about itself properly:
            assertEquals(remoteInputHost, self.getSiteToSiteHostname());
            assertEquals(remoteSocketPort, self.getSiteToSitePort().intValue());
            assertNull(self.getSiteToSiteHttpApiPort());
            assertEquals(apiPort, self.getAPIPort());
            return null;
        }).when(serverProtocol).sendPeerList(any(Peer.class), any(Optional.class), any(NodeInformation.class));

        final Peer peer = null;
        method.invoke(listener, serverProtocol, peer, RequestType.REQUEST_PEER_LIST);

    }

}
