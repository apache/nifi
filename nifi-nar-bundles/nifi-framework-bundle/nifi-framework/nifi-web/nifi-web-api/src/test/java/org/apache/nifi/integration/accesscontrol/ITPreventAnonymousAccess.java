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

import org.apache.nifi.integration.util.SourceTestProcessor;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ws.rs.core.Response;

/**
 * Integration test for preventing anonymous access.
 */
public class ITPreventAnonymousAccess {

    private static OneWaySslAccessControlHelper helper;

    private static final String CLIENT_ID = "anonymous-client-id";

    @BeforeClass
    public static void setup() throws Exception {
        helper = new OneWaySslAccessControlHelper();
    }

    /**
     * Attempt to create a processor anonymously.
     *
     * @throws Exception ex
     */
    @Test
    public void testCreateProcessorUsingToken() throws Exception {
        String url = helper.getBaseUrl() + "/process-groups/root/processors";

        // create the processor
        ProcessorDTO processor = new ProcessorDTO();
        processor.setName("Copy");
        processor.setType(SourceTestProcessor.class.getName());

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(0l);

        // create the entity body
        ProcessorEntity entity = new ProcessorEntity();
        entity.setRevision(revision);
        entity.setComponent(processor);

        // perform the request
        Response response = helper.getUser().testPost(url, entity);

        // ensure the request is rejected
        Assert.assertEquals(401, response.getStatus());
    }

    @AfterClass
    public static void cleanup() throws Exception {
        helper.cleanup();
    }
}
