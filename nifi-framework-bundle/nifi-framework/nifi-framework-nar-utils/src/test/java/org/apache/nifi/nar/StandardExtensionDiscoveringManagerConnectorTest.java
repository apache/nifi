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
package org.apache.nifi.nar;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.components.connector.Connector;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StandardExtensionDiscoveringManagerConnectorTest {

    @Test
    public void testConnectorDiscoveryFromServiceFile() {
        final NiFiProperties properties = NiFiProperties.createBasicNiFiProperties("src/test/resources/nifi.properties");
        final Bundle systemBundle = SystemBundle.create(properties);

        final StandardExtensionDiscoveringManager manager = new StandardExtensionDiscoveringManager();
        manager.discoverExtensions(systemBundle, Collections.emptySet());

        final Set<ExtensionDefinition> connectorDefs = manager.getExtensions(Connector.class);
        final ExtensionDefinition connectorDefinition = connectorDefs.stream()
            .filter(def -> def.getImplementationClassName().equals("org.apache.nifi.nar.DummyConnector"))
            .findFirst().orElseThrow(() -> new AssertionError("Did not find expected DummyConnector"));

        assertEquals(systemBundle, connectorDefinition.getBundle());
        assertEquals(Connector.class, connectorDefinition.getExtensionType());
    }
}


