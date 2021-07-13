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

package org.apache.nifi.stateless.config;

import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.stateless.engine.StatelessEngineConfiguration;
import org.apache.nifi.stateless.flow.DataflowDefinition;
import org.apache.nifi.stateless.flow.TransactionThresholds;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestPropertiesFileFlowDefinitionParser {

    @Test
    public void testParse() throws IOException, StatelessConfigurationException {
        final PropertiesFileFlowDefinitionParser parser = new PropertiesFileFlowDefinitionParser();

        final List<ParameterOverride> parameterOverrides = new ArrayList<>();
        final StatelessEngineConfiguration engineConfig = createStatelessEngineConfiguration();
        final DataflowDefinition<?> dataflowDefinition = parser.parseFlowDefinition(new File("src/test/resources/flow-configuration.properties"), engineConfig, parameterOverrides);
        assertEquals(new HashSet<>(Arrays.asList("foo", "bar", "baz")), dataflowDefinition.getFailurePortNames());

        final List<ParameterContextDefinition> contextDefinitions = dataflowDefinition.getParameterContexts();
        assertEquals(2, contextDefinitions.size());

        final ParameterContextDefinition first = contextDefinitions.get(0);
        assertEquals("My Context", first.getName());

        final List<ParameterDefinition> firstParams = first.getParameters();
        assertEquals(1, firstParams.size());
        assertEquals("File Size", firstParams.get(0).getName());
        assertEquals("25 B", firstParams.get(0).getValue());

        final ParameterContextDefinition second = contextDefinitions.get(1);
        assertEquals("Other Context", second.getName());

        final List<ParameterDefinition> secondParams = second.getParameters();
        assertEquals(2, secondParams.size());
        assertEquals("File Size", secondParams.get(0).getName());
        assertEquals("25 KB", secondParams.get(0).getValue());

        assertEquals("duration", secondParams.get(1).getName());
        assertEquals("48", secondParams.get(1).getValue());

        final TransactionThresholds transactionThresholds = dataflowDefinition.getTransactionThresholds();
        assertNotNull(transactionThresholds);
        assertEquals(1000, transactionThresholds.getMaxFlowFiles().getAsLong());
        assertEquals(4L, transactionThresholds.getMaxContentSize(DataUnit.KB).getAsLong());
        assertEquals(1000L, transactionThresholds.getMaxTime(TimeUnit.MILLISECONDS).getAsLong());
    }

    private StatelessEngineConfiguration createStatelessEngineConfiguration() {
        return new StatelessEngineConfiguration() {
            @Override
            public File getWorkingDirectory() {
                return null;
            }

            @Override
            public File getNarDirectory() {
                return null;
            }

            @Override
            public File getExtensionsDirectory() {
                return null;
            }

            @Override
            public File getKrb5File() {
                return null;
            }

            @Override
            public SslContextDefinition getSslContext() {
                return null;
            }

            @Override
            public String getSensitivePropsKey() {
                return null;
            }

            @Override
            public List<ExtensionClientDefinition> getExtensionClients() {
                return Collections.emptyList();
            }
        };
    }
}
