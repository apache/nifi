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
package org.apache.nifi.web.api;

import org.apache.nifi.remote.protocol.ResponseCode;
import org.apache.nifi.remote.protocol.http.HttpHeaders;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.entity.ControllerEntity;
import org.apache.nifi.web.api.entity.PeersEntity;
import org.apache.nifi.web.api.entity.TransactionResultEntity;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class TestSiteToSiteResource {

    @BeforeClass
    public static void setup() throws Exception {
        final URL resource = TestSiteToSiteResource.class.getResource("/site-to-site/nifi.properties");
        final String propertiesFile = resource.toURI().getPath();
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, propertiesFile);
    }

    @Test
    public void testGetControllerForOlderVersion() throws Exception {
        final HttpServletRequest req = mock(HttpServletRequest.class);
        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);
        final ControllerEntity controllerEntity = new ControllerEntity();
        final ControllerDTO controller = new ControllerDTO();
        controllerEntity.setController(controller);

        controller.setRemoteSiteHttpListeningPort(8080);
        controller.setRemoteSiteListeningPort(9990);

        doReturn(controller).when(serviceFacade).getSiteToSiteDetails();

        final SiteToSiteResource resource = getSiteToSiteResource(serviceFacade);
        final Response response = resource.getSiteToSiteDetails(req);

        ControllerEntity resultEntity = (ControllerEntity)response.getEntity();

        assertEquals(200, response.getStatus());
        assertNull("remoteSiteHttpListeningPort should be null since older version doesn't recognize this field" +
                " and throws JSON mapping exception.", resultEntity.getController().getRemoteSiteHttpListeningPort());
        assertEquals("Other fields should be retained.", new Integer(9990), controllerEntity.getController().getRemoteSiteListeningPort());
    }

    @Test
    public void testGetController() throws Exception {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);
        final ControllerEntity controllerEntity = new ControllerEntity();
        final ControllerDTO controller = new ControllerDTO();
        controllerEntity.setController(controller);

        controller.setRemoteSiteHttpListeningPort(8080);
        controller.setRemoteSiteListeningPort(9990);

        doReturn(controller).when(serviceFacade).getSiteToSiteDetails();

        final SiteToSiteResource resource = getSiteToSiteResource(serviceFacade);
        final Response response = resource.getSiteToSiteDetails(req);

        ControllerEntity resultEntity = (ControllerEntity)response.getEntity();

        assertEquals(200, response.getStatus());
        assertEquals("remoteSiteHttpListeningPort should be retained", new Integer(8080), resultEntity.getController().getRemoteSiteHttpListeningPort());
        assertEquals("Other fields should be retained.", new Integer(9990), controllerEntity.getController().getRemoteSiteListeningPort());
    }

    private HttpServletRequest createCommonHttpServletRequest() {
        final HttpServletRequest req = mock(HttpServletRequest.class);
        doReturn("1").when(req).getHeader(eq(HttpHeaders.PROTOCOL_VERSION));
        return req;
    }

    @Test
    public void testPeers() throws Exception {
        final HttpServletRequest req = createCommonHttpServletRequest();

        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);

        final SiteToSiteResource resource = getSiteToSiteResource(serviceFacade);

        final Response response = resource.getPeers(req);

        PeersEntity resultEntity = (PeersEntity) response.getEntity();

        assertEquals(200, response.getStatus());
        assertEquals(1, resultEntity.getPeers().size());
    }


    @Test
    public void testPeersVersionWasNotSpecified() throws Exception {
        final HttpServletRequest req = mock(HttpServletRequest.class);

        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);

        final SiteToSiteResource resource = getSiteToSiteResource(serviceFacade);

        final Response response = resource.getPeers(req);

        TransactionResultEntity resultEntity = (TransactionResultEntity) response.getEntity();
        assertEquals(400, response.getStatus());
        assertEquals(ResponseCode.ABORT.getCode(), resultEntity.getResponseCode());
    }

    @Test
    public void testPeersVersionNegotiationDowngrade() throws Exception {
        final HttpServletRequest req = mock(HttpServletRequest.class);
        doReturn("999").when(req).getHeader(eq(HttpHeaders.PROTOCOL_VERSION));

        final NiFiServiceFacade serviceFacade = mock(NiFiServiceFacade.class);

        final SiteToSiteResource resource = getSiteToSiteResource(serviceFacade);

        final Response response = resource.getPeers(req);

        PeersEntity resultEntity = (PeersEntity) response.getEntity();

        assertEquals(200, response.getStatus());
        assertEquals(1, resultEntity.getPeers().size());
        assertEquals(new Integer(1), response.getMetadata().getFirst(HttpHeaders.PROTOCOL_VERSION));
    }

    private SiteToSiteResource getSiteToSiteResource(final NiFiServiceFacade serviceFacade) {
        final SiteToSiteResource resource = new SiteToSiteResource(NiFiProperties.createBasicNiFiProperties(null, null)) {
            @Override
            protected void authorizeSiteToSite() {
            }
        };
        resource.setProperties(NiFiProperties.createBasicNiFiProperties(null, null));
        resource.setServiceFacade(serviceFacade);
        return resource;
    }
}