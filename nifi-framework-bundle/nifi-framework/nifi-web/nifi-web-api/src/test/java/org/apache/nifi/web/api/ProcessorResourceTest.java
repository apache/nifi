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

import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.AuthorizeAccess;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.apache.nifi.components.BacklogReportingException;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.BacklogDTO;
import org.apache.nifi.web.api.entity.BacklogEntity;
import org.apache.nifi.web.api.entity.BacklogRequestEntity;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ProcessorResourceTest {

    @InjectMocks
    private ProcessorResource processorResource;

    @Mock
    private NiFiServiceFacade serviceFacade;

    @Mock
    private Authorizer authorizer;

    @Mock
    private HttpServletRequest httpServletRequest;

    @Mock
    private ServletContext servletContext;

    @Mock
    private NiFiProperties properties;

    @Mock
    private UriInfo uriInfo;

    @Mock
    private UriBuilder uriBuilder;

    private static final String PROCESSOR_ID = "test-processor-id";

    @BeforeEach
    public void setUp() throws Exception {
        lenient().when(httpServletRequest.getHeader(any())).thenReturn(null);
        lenient().when(httpServletRequest.getServletContext()).thenReturn(servletContext);
        lenient().when(httpServletRequest.getContextPath()).thenReturn("/nifi-api");
        lenient().when(httpServletRequest.getScheme()).thenReturn("http");
        lenient().when(httpServletRequest.getServerName()).thenReturn("localhost");
        lenient().when(httpServletRequest.getServerPort()).thenReturn(8080);
        lenient().when(servletContext.getInitParameter(any())).thenReturn(null);
        lenient().when(properties.isNode()).thenReturn(Boolean.FALSE);

        lenient().when(uriInfo.getBaseUriBuilder()).thenReturn(uriBuilder);
        lenient().when(uriBuilder.segment(any(String[].class))).thenReturn(uriBuilder);
        lenient().when(uriBuilder.build()).thenReturn(new URI("http://localhost:8080/nifi-api/processors/" + PROCESSOR_ID));

        processorResource.setServiceFacade(serviceFacade);
        processorResource.setAuthorizer(authorizer);
        processorResource.httpServletRequest = httpServletRequest;
        processorResource.properties = properties;
        processorResource.uriInfo = uriInfo;
    }

    @AfterEach
    public void tearDown() {
        SecurityContextHolder.clearContext();
    }

    private NiFiUser authenticate() {
        final NiFiUser user = new StandardNiFiUser.Builder().identity("unit-test-user").build();
        final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(user));
        SecurityContextHolder.getContext().setAuthentication(authentication);
        return user;
    }

    @Test
    public void testSubmitProcessorBacklogRequestHappyPath() throws Exception {
        authenticate();

        final BacklogEntity entity = new BacklogEntity();
        final BacklogDTO dto = new BacklogDTO();
        dto.setRecordCount(42L);
        dto.setPrecision("EXACT");
        entity.setBacklog(dto);
        when(serviceFacade.getProcessorBacklog(PROCESSOR_ID)).thenReturn(entity);

        final String requestId;
        try (Response response = processorResource.submitProcessorBacklogRequest(PROCESSOR_ID)) {
            assertEquals(200, response.getStatus());
            final BacklogRequestEntity body = (BacklogRequestEntity) response.getEntity();
            assertEquals(PROCESSOR_ID, body.getRequest().getComponentId());
            assertNotNull(body.getRequest().getRequestId());
            requestId = body.getRequest().getRequestId();
        }

        verify(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));
        verify(serviceFacade).verifyCanReportProcessorBacklog(PROCESSOR_ID);

        final BacklogRequestEntity completedEntity = awaitBacklogRequestCompletion(requestId);
        assertEquals(42L, completedEntity.getRequest().getBacklog().getRecordCount());

        try (Response deleteResponse = processorResource.deleteBacklogRequest(PROCESSOR_ID, requestId)) {
            assertEquals(200, deleteResponse.getStatus());
        }
    }

    @Test
    public void testSubmitProcessorBacklogRequestSurfacesRootCauseOfFailure() throws Exception {
        authenticate();

        final RuntimeException rootCause = new RuntimeException("Timed out waiting for a node assignment");
        when(serviceFacade.getProcessorBacklog(PROCESSOR_ID))
                .thenThrow(new BacklogReportingException("Failed to determine Kafka backlog", rootCause));

        final String requestId;
        try (Response response = processorResource.submitProcessorBacklogRequest(PROCESSOR_ID)) {
            requestId = ((BacklogRequestEntity) response.getEntity()).getRequest().getRequestId();
        }

        final BacklogRequestEntity completedEntity = awaitBacklogRequestCompletion(requestId);
        final String failureReason = completedEntity.getRequest().getFailureReason();
        assertTrue(failureReason.contains("Failed to determine Kafka backlog"));
        assertTrue(failureReason.contains("Timed out waiting for a node assignment"));
    }

    @Test
    public void testSubmitProcessorBacklogRequestVerificationFailsNeverCallsServiceFacade() throws Exception {
        authenticate();
        doThrow(new IllegalStateException("Processor is disabled")).when(serviceFacade).verifyCanReportProcessorBacklog(PROCESSOR_ID);

        assertThrows(IllegalStateException.class, () -> processorResource.submitProcessorBacklogRequest(PROCESSOR_ID));

        verify(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));
        verify(serviceFacade).verifyCanReportProcessorBacklog(PROCESSOR_ID);
        verify(serviceFacade, never()).getProcessorBacklog(anyString());
    }

    @Test
    public void testSubmitProcessorBacklogRequestNotAuthorized() throws Exception {
        authenticate();
        doThrow(AccessDeniedException.class).when(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));

        assertThrows(AccessDeniedException.class, () -> processorResource.submitProcessorBacklogRequest(PROCESSOR_ID));

        verify(serviceFacade, never()).verifyCanReportProcessorBacklog(anyString());
    }

    @Test
    public void testDeleteBacklogRequestCancelsInProgressRequestAndInterruptsBackgroundThread() throws Exception {
        authenticate();

        final CountDownLatch backlogDeterminationStarted = new CountDownLatch(1);
        final CountDownLatch backlogDeterminationInterrupted = new CountDownLatch(1);
        when(serviceFacade.getProcessorBacklog(PROCESSOR_ID)).thenAnswer(invocation -> {
            backlogDeterminationStarted.countDown();
            try {
                Thread.sleep(30_000L);
            } catch (final InterruptedException e) {
                backlogDeterminationInterrupted.countDown();
                throw new BacklogReportingException("Interrupted while determining backlog");
            }
            return new BacklogEntity();
        });

        final String requestId;
        try (Response response = processorResource.submitProcessorBacklogRequest(PROCESSOR_ID)) {
            requestId = ((BacklogRequestEntity) response.getEntity()).getRequest().getRequestId();
        }

        assertTrue(backlogDeterminationStarted.await(5, TimeUnit.SECONDS));

        try (Response deleteResponse = processorResource.deleteBacklogRequest(PROCESSOR_ID, requestId)) {
            assertEquals(200, deleteResponse.getStatus());
            final BacklogRequestEntity body = (BacklogRequestEntity) deleteResponse.getEntity();
            assertTrue(body.getRequest().isComplete());
            assertEquals("Request cancelled by user", body.getRequest().getFailureReason());
        }

        assertTrue(backlogDeterminationInterrupted.await(5, TimeUnit.SECONDS));
    }

    private BacklogRequestEntity awaitBacklogRequestCompletion(final String requestId) throws Exception {
        final long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
        BacklogRequestEntity entity;
        do {
            try (Response response = processorResource.getBacklogRequest(PROCESSOR_ID, requestId)) {
                entity = (BacklogRequestEntity) response.getEntity();
            }
            if (entity.getRequest().isComplete()) {
                return entity;
            }
            Thread.sleep(20L);
        } while (System.currentTimeMillis() < deadline);

        fail("Backlog request did not complete within the expected time");
        return entity;
    }
}
