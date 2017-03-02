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
package org.apache.nifi.integration.accesscontrol;

import com.sun.jersey.api.client.ClientResponse;
import org.apache.nifi.integration.util.NiFiTestAuthorizer;
import org.apache.nifi.integration.util.NiFiTestUser;
import org.apache.nifi.integration.util.RestrictedProcessor;
import org.apache.nifi.integration.util.SourceTestProcessor;
import org.apache.nifi.util.Tuple;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.SnippetDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.entity.CopySnippetRequestEntity;
import org.apache.nifi.web.api.entity.CreateTemplateRequestEntity;
import org.apache.nifi.web.api.entity.FlowEntity;
import org.apache.nifi.web.api.entity.InstantiateTemplateRequestEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.SnippetEntity;
import org.apache.nifi.web.api.entity.TemplateEntity;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.integration.accesscontrol.AccessControlHelper.NONE_CLIENT_ID;
import static org.apache.nifi.integration.accesscontrol.AccessControlHelper.READ_CLIENT_ID;
import static org.apache.nifi.integration.accesscontrol.AccessControlHelper.READ_WRITE_CLIENT_ID;
import static org.apache.nifi.integration.accesscontrol.AccessControlHelper.WRITE_CLIENT_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Access control test for processors.
 */
public class ITProcessorAccessControl {

    private static AccessControlHelper helper;

    @BeforeClass
    public static void setup() throws Exception {
        helper = new AccessControlHelper();
    }

    /**
     * Ensures the READ user can get a processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadUserGetProcessor() throws Exception {
        final ProcessorEntity entity = getRandomProcessor(helper.getReadUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());
    }

    /**
     * Ensures the READ WRITE user can get a processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadWriteUserGetProcessor() throws Exception {
        final ProcessorEntity entity = getRandomProcessor(helper.getReadWriteUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertTrue(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());
    }

    /**
     * Ensures the WRITE user can get a processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testWriteUserGetProcessor() throws Exception {
        final ProcessorEntity entity = getRandomProcessor(helper.getWriteUser());
        assertFalse(entity.getPermissions().getCanRead());
        assertTrue(entity.getPermissions().getCanWrite());
        assertNull(entity.getComponent());
    }

    /**
     * Ensures the NONE user can get a processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testNoneUserGetProcessor() throws Exception {
        final ProcessorEntity entity = getRandomProcessor(helper.getNoneUser());
        assertFalse(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNull(entity.getComponent());
    }

    /**
     * Ensures the READ user cannot put a processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadUserPutProcessor() throws Exception {
        final ProcessorEntity entity = getRandomProcessor(helper.getReadUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());

        // attempt update the name
        entity.getRevision().setClientId(READ_CLIENT_ID);
        entity.getComponent().setName("Updated Name");

        // perform the request
        final ClientResponse response = updateProcessor(helper.getReadUser(), entity);

        // ensure forbidden response
        assertEquals(403, response.getStatus());
    }

    /**
     * Ensures the READ_WRITE user can put a processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadWriteUserPutProcessor() throws Exception {
        final ProcessorEntity entity = getRandomProcessor(helper.getReadWriteUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertTrue(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());

        final String updatedName = "Updated Name";

        // attempt to update the name
        final long version = entity.getRevision().getVersion();
        entity.getRevision().setClientId(AccessControlHelper.READ_WRITE_CLIENT_ID);
        entity.getComponent().setName(updatedName);

        // perform the request
        final ClientResponse response = updateProcessor(helper.getReadWriteUser(), entity);

        // ensure successful response
        assertEquals(200, response.getStatus());

        // get the response
        final ProcessorEntity responseEntity = response.getEntity(ProcessorEntity.class);

        // verify
        assertEquals(READ_WRITE_CLIENT_ID, responseEntity.getRevision().getClientId());
        assertEquals(version + 1, responseEntity.getRevision().getVersion().longValue());
        assertEquals(updatedName, responseEntity.getComponent().getName());
    }

    /**
     * Ensures the READ_WRITE user can put a processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadWriteUserPutProcessorThroughInheritedPolicy() throws Exception {
        final ProcessorEntity entity = createProcessor(helper, NiFiTestAuthorizer.NO_POLICY_COMPONENT_NAME);

        final String updatedName = "Updated name";

        // attempt to update the name
        final long version = entity.getRevision().getVersion();
        entity.getRevision().setClientId(READ_WRITE_CLIENT_ID);
        entity.getComponent().setName(updatedName);

        // perform the request
        final ClientResponse response = updateProcessor(helper.getReadWriteUser(), entity);

        // ensure successful response
        assertEquals(200, response.getStatus());

        // get the response
        final ProcessorEntity responseEntity = response.getEntity(ProcessorEntity.class);

        // verify
        assertEquals(AccessControlHelper.READ_WRITE_CLIENT_ID, responseEntity.getRevision().getClientId());
        assertEquals(version + 1, responseEntity.getRevision().getVersion().longValue());
        assertEquals(updatedName, responseEntity.getComponent().getName());
    }

    /**
     * Ensures the WRITE user can put a processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testWriteUserPutProcessor() throws Exception {
        final ProcessorEntity entity = getRandomProcessor(helper.getWriteUser());
        assertFalse(entity.getPermissions().getCanRead());
        assertTrue(entity.getPermissions().getCanWrite());
        assertNull(entity.getComponent());

        final String updatedName = "Updated Name";

        // attempt to update the name
        final ProcessorDTO requestDto = new ProcessorDTO();
        requestDto.setId(entity.getId());
        requestDto.setName(updatedName);

        final long version = entity.getRevision().getVersion();
        final RevisionDTO requestRevision = new RevisionDTO();
        requestRevision.setVersion(version);
        requestRevision.setClientId(AccessControlHelper.WRITE_CLIENT_ID);

        final ProcessorEntity requestEntity = new ProcessorEntity();
        requestEntity.setId(entity.getId());
        requestEntity.setRevision(requestRevision);
        requestEntity.setComponent(requestDto);

        // perform the request
        final ClientResponse response = updateProcessor(helper.getWriteUser(), requestEntity);

        // ensure successful response
        assertEquals(200, response.getStatus());

        // get the response
        final ProcessorEntity responseEntity = response.getEntity(ProcessorEntity.class);

        // verify
        assertEquals(WRITE_CLIENT_ID, responseEntity.getRevision().getClientId());
        assertEquals(version + 1, responseEntity.getRevision().getVersion().longValue());
    }

    /**
     * Ensures the NONE user cannot put a processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testNoneUserPutProcessor() throws Exception {
        final ProcessorEntity entity = getRandomProcessor(helper.getNoneUser());
        assertFalse(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNull(entity.getComponent());

        final String updatedName = "Updated Name";

        // attempt to update the name
        final ProcessorDTO requestDto = new ProcessorDTO();
        requestDto.setId(entity.getId());
        requestDto.setName(updatedName);

        final long version = entity.getRevision().getVersion();
        final RevisionDTO requestRevision = new RevisionDTO();
        requestRevision.setVersion(version);
        requestRevision.setClientId(AccessControlHelper.NONE_CLIENT_ID);

        final ProcessorEntity requestEntity = new ProcessorEntity();
        requestEntity.setId(entity.getId());
        requestEntity.setRevision(requestRevision);
        requestEntity.setComponent(requestDto);

        // perform the request
        final ClientResponse response = updateProcessor(helper.getNoneUser(), requestEntity);

        // ensure forbidden response
        assertEquals(403, response.getStatus());
    }

    /**
     * Ensures the READ user cannot clear state.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadUserClearState() throws Exception {
        final ProcessorEntity entity = getRandomProcessor(helper.getReadUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());

        final String url = helper.getBaseUrl() + "/processors/" + entity.getId() + "/state/clear-requests";

        // perform the request
        final ClientResponse response = helper.getReadUser().testPost(url);

        // ensure forbidden response
        assertEquals(403, response.getStatus());
    }

    /**
     * Ensures the NONE user cannot clear state.
     *
     * @throws Exception ex
     */
    @Test
    public void testNoneUserClearState() throws Exception {
        final ProcessorEntity entity = getRandomProcessor(helper.getReadUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());

        final String url = helper.getBaseUrl() + "/processors/" + entity.getId() + "/state/clear-requests";

        // perform the request
        final ClientResponse response = helper.getNoneUser().testPost(url);

        // ensure forbidden response
        assertEquals(403, response.getStatus());
    }

    /**
     * Ensures the READ WRITE user can clear state.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadWriteUserClearState() throws Exception {
        final ProcessorEntity entity = getRandomProcessor(helper.getReadUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());

        final String url = helper.getBaseUrl() + "/processors/" + entity.getId() + "/state/clear-requests";

        // perform the request
        final ClientResponse response = helper.getReadWriteUser().testPost(url);

        // ensure ok response
        assertEquals(200, response.getStatus());
    }

    /**
     * Ensures the WRITE user can clear state.
     *
     * @throws Exception ex
     */
    @Test
    public void testWriteUserClearState() throws Exception {
        final ProcessorEntity entity = getRandomProcessor(helper.getReadUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());

        final String url = helper.getBaseUrl() + "/processors/" + entity.getId() + "/state/clear-requests";

        // perform the request
        final ClientResponse response = helper.getWriteUser().testPost(url);

        // ensure ok response
        assertEquals(200, response.getStatus());
    }

    /**
     * Ensures the READ user cannot delete a processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadUserDeleteProcessor() throws Exception {
        verifyDelete(helper.getReadUser(), AccessControlHelper.READ_CLIENT_ID, 403);
    }

    /**
     * Ensures the READ WRITE user can delete a processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadWriteUserDeleteProcessor() throws Exception {
        verifyDelete(helper.getReadWriteUser(), AccessControlHelper.READ_WRITE_CLIENT_ID, 200);
    }

    /**
     * Ensures the WRITE user can delete a processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testWriteUserDeleteProcessor() throws Exception {
        verifyDelete(helper.getWriteUser(), AccessControlHelper.WRITE_CLIENT_ID, 200);
    }

    /**
     * Ensures the NONE user can delete a processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testNoneUserDeleteProcessor() throws Exception {
        verifyDelete(helper.getNoneUser(), NONE_CLIENT_ID, 403);
    }

    /**
     * Tests attempt to create a restricted processor.
     *
     * @throws Exception if there is an error creating this processor
     */
    @Test
    public void testCreateRestrictedProcessor() throws Exception {
        String url = helper.getBaseUrl() + "/process-groups/root/processors";

        // create the processor
        ProcessorDTO processor = new ProcessorDTO();
        processor.setName("restricted");
        processor.setType(RestrictedProcessor.class.getName());

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(READ_WRITE_CLIENT_ID);
        revision.setVersion(0L);

        // create the entity body
        ProcessorEntity entity = new ProcessorEntity();
        entity.setRevision(revision);
        entity.setComponent(processor);

        // perform the request as a user with read/write but no restricted access
        ClientResponse response = helper.getReadWriteUser().testPost(url, entity);

        // ensure the request is successful
        assertEquals(403, response.getStatus());

        // perform the request as a user with read/write and restricted access
        response = helper.getPrivilegedUser().testPost(url, entity);

        // ensure the request is successful
        assertEquals(201, response.getStatus());

        final ProcessorEntity responseEntity = response.getEntity(ProcessorEntity.class);

        // remove the restricted component
        deleteRestrictedComponent(responseEntity);
    }

    /**
     * Tests attempting to copy/paste a restricted processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testCopyPasteRestrictedProcessor() throws Exception {
        final String copyUrl = helper.getBaseUrl() + "/process-groups/root/snippet-instance";
        final Tuple<ProcessorEntity, SnippetEntity> tuple = createSnippetWithRestrictedComponent();
        final SnippetEntity snippetEntity = tuple.getValue();

        // build the copy/paste request
        final CopySnippetRequestEntity copyRequest = new CopySnippetRequestEntity();
        copyRequest.setSnippetId(snippetEntity.getSnippet().getId());
        copyRequest.setOriginX(0.0);
        copyRequest.setOriginY(0.0);

        // create the snippet
        ClientResponse response = helper.getReadWriteUser().testPost(copyUrl, copyRequest);

        // ensure the request failed... need privileged users since snippet comprised of the restricted components
        assertEquals(403, response.getStatus());

        // create the snippet
        response = helper.getPrivilegedUser().testPost(copyUrl, copyRequest);

        // ensure the request is successful
        assertEquals(201, response.getStatus());

        final FlowEntity flowEntity = response.getEntity(FlowEntity.class);

        // remove the restricted processors
        deleteRestrictedComponent(tuple.getKey());
        deleteRestrictedComponent(flowEntity.getFlow().getProcessors().stream().findFirst().orElse(null));
    }

    /**
     * Tests attempting to use a template with a restricted processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testTemplateWithRestrictedProcessor() throws Exception {
        final String createTemplateUrl = helper.getBaseUrl() + "/process-groups/root/templates";
        final String instantiateTemplateUrl = helper.getBaseUrl() + "/process-groups/root/template-instance";
        final Tuple<ProcessorEntity, SnippetEntity> tuple = createSnippetWithRestrictedComponent();
        final SnippetEntity snippetEntity = tuple.getValue();

        // create the template
        final CreateTemplateRequestEntity createTemplateRequest = new CreateTemplateRequestEntity();
        createTemplateRequest.setSnippetId(snippetEntity.getSnippet().getId());
        createTemplateRequest.setName("test");

        // create the snippet
        ClientResponse response = helper.getWriteUser().testPost(createTemplateUrl, createTemplateRequest);

        // ensure the request failed... need read perms to the components in the snippet
        assertEquals(403, response.getStatus());

        response = helper.getReadWriteUser().testPost(createTemplateUrl, createTemplateRequest);

        // ensure the request is successfull
        assertEquals(201, response.getStatus());

        final TemplateEntity templateEntity = response.getEntity(TemplateEntity.class);

        // build the template request
        final InstantiateTemplateRequestEntity instantiateTemplateRequest = new InstantiateTemplateRequestEntity();
        instantiateTemplateRequest.setTemplateId(templateEntity.getTemplate().getId());
        instantiateTemplateRequest.setOriginX(0.0);
        instantiateTemplateRequest.setOriginY(0.0);

        // create the snippet
        response = helper.getReadWriteUser().testPost(instantiateTemplateUrl, instantiateTemplateRequest);

        // ensure the request failed... need privileged user since the template is comprised of restricted components
        assertEquals(403, response.getStatus());

        // create the snippet
        response = helper.getPrivilegedUser().testPost(instantiateTemplateUrl, instantiateTemplateRequest);

        // ensure the request is successful
        assertEquals(201, response.getStatus());

        final FlowEntity flowEntity = response.getEntity(FlowEntity.class);

        // clean up the resources created during this test
        deleteTemplate(templateEntity);
        deleteRestrictedComponent(tuple.getKey());
        deleteRestrictedComponent(flowEntity.getFlow().getProcessors().stream().findFirst().orElse(null));
    }

    private Tuple<ProcessorEntity, SnippetEntity> createSnippetWithRestrictedComponent() throws Exception {
        final String processorUrl = helper.getBaseUrl() + "/process-groups/root/processors";
        final String snippetUrl = helper.getBaseUrl() + "/snippets";

        // create the processor
        ProcessorDTO processor = new ProcessorDTO();
        processor.setName("restricted");
        processor.setType(RestrictedProcessor.class.getName());

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(READ_WRITE_CLIENT_ID);
        revision.setVersion(0L);

        // create the entity body
        ProcessorEntity entity = new ProcessorEntity();
        entity.setRevision(revision);
        entity.setComponent(processor);

        // perform the request as a user with read/write and restricted access
        ClientResponse response = helper.getPrivilegedUser().testPost(processorUrl, entity);

        // ensure the request is successful
        assertEquals(201, response.getStatus());

        // get the response
        final ProcessorEntity responseProcessorEntity = response.getEntity(ProcessorEntity.class);

        // build the snippet for the copy/paste
        final SnippetDTO snippet = new SnippetDTO();
        snippet.setParentGroupId(responseProcessorEntity.getComponent().getParentGroupId());
        snippet.getProcessors().put(responseProcessorEntity.getId(), responseProcessorEntity.getRevision());

        // create the entity body
        final SnippetEntity snippetEntity = new SnippetEntity();
        snippetEntity.setSnippet(snippet);

        // create the snippet
        response = helper.getNoneUser().testPost(snippetUrl, snippetEntity);

        // ensure the request failed... need either read or write to create snippet (not sure what snippet will be used for)
        assertEquals(403, response.getStatus());

        // create the snippet
        response = helper.getReadWriteUser().testPost(snippetUrl, snippetEntity);

        // ensure the request is successful
        assertEquals(201, response.getStatus());

        // get the response
        return new Tuple<>(responseProcessorEntity, response.getEntity(SnippetEntity.class));
    }

    private void deleteTemplate(final TemplateEntity entity) throws Exception {
        // perform the request
        ClientResponse response = helper.getReadWriteUser().testDelete(entity.getTemplate().getUri());

        // ensure the request is successful
        assertEquals(200, response.getStatus());
    }

    private void deleteRestrictedComponent(final ProcessorEntity entity) throws Exception {
        if (entity == null) {
            Assert.fail("Failed to get Processor from template or snippet request.");
            return;
        }

        // create the entity body
        final Map<String, String> queryParams = new HashMap<>();
        queryParams.put("version", String.valueOf(entity.getRevision().getVersion()));
        queryParams.put("clientId", READ_WRITE_CLIENT_ID);

        // perform the request
        ClientResponse response = helper.getReadWriteUser().testDelete(entity.getUri(), queryParams);

        // ensure the request fails... needs access to restricted components
        assertEquals(403, response.getStatus());

        response = helper.getPrivilegedUser().testDelete(entity.getUri(), queryParams);

        // ensure the request is successful
        assertEquals(200, response.getStatus());
    }

    private ProcessorEntity getRandomProcessor(final NiFiTestUser user) throws Exception {
        final String url = helper.getBaseUrl() + "/flow/process-groups/root";

        // get the processors
        final ClientResponse response = user.testGet(url);

        // ensure the response was successful
        assertEquals(200, response.getStatus());

        // unmarshal
        final ProcessGroupFlowEntity flowEntity = response.getEntity(ProcessGroupFlowEntity.class);
        final FlowDTO flowDto = flowEntity.getProcessGroupFlow().getFlow();
        final Set<ProcessorEntity> processors = flowDto.getProcessors();

        // ensure the correct number of processors
        assertFalse(processors.isEmpty());

        // use the first processor as the target
        Iterator<ProcessorEntity> processorIter = processors.iterator();
        assertTrue(processorIter.hasNext());
        return processorIter.next();
    }

    private ClientResponse updateProcessor(final NiFiTestUser user, final ProcessorEntity entity) throws Exception {
        final String url = helper.getBaseUrl() + "/processors/" + entity.getId();

        // perform the request
        return user.testPut(url, entity);
    }

    public static ProcessorEntity createProcessor(final AccessControlHelper ach, final String name) throws Exception {
        String url = ach.getBaseUrl() + "/process-groups/root/processors";

        // create the processor
        ProcessorDTO processor = new ProcessorDTO();
        processor.setName(name);
        processor.setType(SourceTestProcessor.class.getName());

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(READ_WRITE_CLIENT_ID);
        revision.setVersion(0L);

        // create the entity body
        ProcessorEntity entity = new ProcessorEntity();
        entity.setRevision(revision);
        entity.setComponent(processor);

        // perform the request
        ClientResponse response = ach.getReadWriteUser().testPost(url, entity);

        // ensure the request is successful
        assertEquals(201, response.getStatus());

        // get the entity body
        entity = response.getEntity(ProcessorEntity.class);

        // verify creation
        processor = entity.getComponent();
        assertEquals(name, processor.getName());
        assertEquals("org.apache.nifi.integration.util.SourceTestProcessor", processor.getType());

        // get the processor
        return entity;
    }

    private void verifyDelete(final NiFiTestUser user, final String clientId, final int responseCode) throws Exception {
        final ProcessorEntity entity = createProcessor(helper, "Copy");

        // create the entity body
        final Map<String, String> queryParams = new HashMap<>();
        queryParams.put("version", String.valueOf(entity.getRevision().getVersion()));
        queryParams.put("clientId", clientId);

        // perform the request
        ClientResponse response = user.testDelete(entity.getUri(), queryParams);

        // ensure the request is failed with a forbidden status code
        assertEquals(responseCode, response.getStatus());
    }

    @AfterClass
    public static void cleanup() throws Exception {
        helper.cleanup();
    }
}
