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
package org.apache.nifi.stateless.core;

import org.apache.nifi.controller.serialization.FlowEncodingVersion;
import org.apache.nifi.controller.serialization.FlowFromDOMFactory;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;


import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FlowXmlTest {

    private NiFiProperties getNiFiProperties() {
        final NiFiProperties nifiProperties = mock(NiFiProperties.class);
        when(nifiProperties.getProperty(StringEncryptor.NF_SENSITIVE_PROPS_ALGORITHM)).thenReturn("PBEWITHMD5AND256BITAES-CBC-OPENSSL");
        when(nifiProperties.getProperty(StringEncryptor.NF_SENSITIVE_PROPS_PROVIDER)).thenReturn("BC");
        when(nifiProperties.getProperty(anyString(), anyString())).then(invocation -> invocation.getArgument(1));
        return nifiProperties;
    }

    @org.junit.Test
    public void testScenario1_Test() throws IOException {

        final NiFiProperties props = getNiFiProperties();
        final StringEncryptor encryptor = NiFiPropertiesUtil.createEncryptorFromProperties(props);

        // parse flow.xml.gz
        final Path flowXmlPath = Paths.get("src/test/resources/flow.xml.gz");
        final Document flow = FlowXmlUtil.readFlowFromDisk(flowXmlPath);

        // grab the root PG Element
        final Element rootElement = flow.getDocumentElement();
        final FlowEncodingVersion encodingVersion = FlowEncodingVersion.parse(rootElement);
        final Element rootGroupElement = (Element) rootElement.getElementsByTagName("rootGroup").item(0);
        final ProcessGroupDTO rootProcessGroup = FlowFromDOMFactory.getProcessGroup(null, rootGroupElement, encryptor, encodingVersion);

        assertNotNull(rootProcessGroup);
        assertNotNull(rootProcessGroup.getContents());

        // flow.xml should only contain 1 PG
        assertEquals(1, rootProcessGroup.getContents().getProcessGroups().size());

        for (ProcessGroupDTO group : rootProcessGroup.getContents().getProcessGroups()) {
            assertNotNull(group.getContents());

            // Check that there are 2 processors
            assertEquals(2, group.getContents().getProcessors().size());

            // Check that there's one processor named AttributesToJSON
            assertEquals(1, group.getContents().getProcessors()
                    .stream().filter(processorDTO -> processorDTO.getName().equals("AttributesToJSON")).count());
        }
    }
}
