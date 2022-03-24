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
package org.apache.nifi.integration.flowfilerepo;

import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.WriteAheadFlowFileRepository;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.integration.DirectInjectionExtensionManager;
import org.apache.nifi.integration.FrameworkIntegrationTest;
import org.apache.nifi.reporting.Bulletin;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

public class OOMEFlowFileRepoUpdateIT extends FrameworkIntegrationTest {
    @Override
    protected Map<String, String> getNiFiPropertiesOverrides() {
        return Collections.singletonMap("nifi.flowfile.repository.implementation", OOMEWriteAheadFlowFileRepository.class.getName());
    }

    @Override
    protected void injectExtensionTypes(final DirectInjectionExtensionManager extensionManager) {
        extensionManager.injectExtensionType(FlowFileRepository.class, OOMEWriteAheadFlowFileRepository.class);
    }

    @Test
    public void testOOMEOnUpdatePreventsSubsequentUpdates() throws ExecutionException, InterruptedException, IOException {
        final ProcessorNode generate = createProcessorNode((context, session) -> {
            FlowFile flowFile = session.create();
            session.transfer(flowFile, REL_SUCCESS);
        }, REL_SUCCESS);
        connect(generate, getTerminateAllProcessor(), REL_SUCCESS);

        for (int i=0; i < 4; i++) {
            triggerOnce(generate);
        }

        List<Bulletin> processorBulletins = getFlowController().getBulletinRepository().findBulletinsForSource(generate.getIdentifier());
        assertEquals(1, processorBulletins.size());

        triggerOnce(generate);

        // FlowFile Repository should not allow us to udpate until it has been checkpointed.
        processorBulletins = getFlowController().getBulletinRepository().findBulletinsForSource(generate.getIdentifier());
        assertEquals(2, processorBulletins.size());

        // Checkpoint the repository.
        ((WriteAheadFlowFileRepository) getFlowFileRepository()).checkpoint();

        // Should now succeed.
        triggerOnce(generate);
        processorBulletins = getFlowController().getBulletinRepository().findBulletinsForSource(generate.getIdentifier());
        assertEquals(2, processorBulletins.size());
    }
}
