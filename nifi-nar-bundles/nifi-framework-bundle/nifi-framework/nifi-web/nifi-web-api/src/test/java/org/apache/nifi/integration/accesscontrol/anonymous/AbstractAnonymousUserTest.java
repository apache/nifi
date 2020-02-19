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
package org.apache.nifi.integration.accesscontrol.anonymous;

import org.apache.nifi.integration.util.NiFiTestUser;
import org.apache.nifi.integration.util.SourceTestProcessor;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ProcessorEntity;

import javax.ws.rs.core.Response;

public abstract class AbstractAnonymousUserTest {

    private static final String CLIENT_ID = "anonymous-client-id";

    /**
     * Attempt to create a processor anonymously.
     *
     * @throws Exception ex
     */
    protected Response testCreateProcessor(final String baseUrl, final NiFiTestUser niFiTestUser) throws Exception {
        final String url = baseUrl + "/process-groups/root/processors";

        // create the processor
        final ProcessorDTO processor = new ProcessorDTO();
        processor.setName("Copy");
        processor.setType(SourceTestProcessor.class.getName());

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(0l);

        // create the entity body
        final ProcessorEntity entity = new ProcessorEntity();
        entity.setRevision(revision);
        entity.setComponent(processor);

        // perform the request
        return niFiTestUser.testPost(url, entity);
    }
}
