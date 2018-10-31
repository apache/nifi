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
package org.apache.nifi.authorization;

import org.apache.nifi.authorization.resource.AccessPolicyAuthorizable;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.DataAuthorizable;
import org.apache.nifi.authorization.resource.DataTransferAuthorizable;
import org.apache.nifi.authorization.resource.OperationAuthorizable;
import org.apache.nifi.authorization.resource.ProvenanceDataAuthorizable;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.web.controller.ControllerFacade;
import org.apache.nifi.web.dao.ProcessorDAO;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StandardAuthorizableLookupTest {

    @Test
    public void testGetAuthorizableFromResource() {
        final ExtensionManager extensionManager = mock(ExtensionDiscoveringManager.class);
        final ControllerFacade controllerFacade = mock(ControllerFacade.class);
        when(controllerFacade.getExtensionManager()).thenReturn(extensionManager);

        final ProcessorDAO processorDAO = mock(ProcessorDAO.class);
        final ProcessorNode processorNode = mock(ProcessorNode.class);

        when(processorDAO.getProcessor(eq("id"))).thenReturn(processorNode);

        final StandardAuthorizableLookup lookup = new StandardAuthorizableLookup();
        lookup.setProcessorDAO(processorDAO);
        lookup.setControllerFacade(controllerFacade);

        Authorizable authorizable = lookup.getAuthorizableFromResource("/processors/id");
        assertTrue(authorizable instanceof ProcessorNode);

        authorizable = lookup.getAuthorizableFromResource("/policies/processors/id");
        assertTrue(authorizable instanceof AccessPolicyAuthorizable);
        assertTrue(((AccessPolicyAuthorizable) authorizable).getBaseAuthorizable() instanceof ProcessorNode);

        authorizable = lookup.getAuthorizableFromResource("/data/processors/id");
        assertTrue(authorizable instanceof DataAuthorizable);
        assertTrue(((DataAuthorizable) authorizable).getBaseAuthorizable() instanceof ProcessorNode);

        authorizable = lookup.getAuthorizableFromResource("/data-transfer/processors/id");
        assertTrue(authorizable instanceof DataTransferAuthorizable);
        assertTrue(((DataTransferAuthorizable) authorizable).getBaseAuthorizable() instanceof ProcessorNode);

        authorizable = lookup.getAuthorizableFromResource("/provenance-data/processors/id");
        assertTrue(authorizable instanceof ProvenanceDataAuthorizable);
        assertTrue(((ProvenanceDataAuthorizable) authorizable).getBaseAuthorizable() instanceof ProcessorNode);

        authorizable = lookup.getAuthorizableFromResource("/operation/processors/id");
        assertTrue(authorizable instanceof OperationAuthorizable);
        assertTrue(((OperationAuthorizable) authorizable).getBaseAuthorizable() instanceof ProcessorNode);
    }
}
