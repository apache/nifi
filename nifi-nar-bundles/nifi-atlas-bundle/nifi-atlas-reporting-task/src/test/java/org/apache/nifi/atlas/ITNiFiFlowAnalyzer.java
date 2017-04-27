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
package org.apache.nifi.atlas;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.List;
import java.util.Properties;

public class ITNiFiFlowAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(ITNiFiFlowAnalyzer.class);

    private NiFiAtlasClient atlasClient;
    private NiFiApiClient nifiClient;
    private AtlasVariables atlasVariables;

    @Before
    public void setup() throws Exception {
        nifiClient = new NiFiApiClient("http://localhost:8080/");

        atlasClient = NiFiAtlasClient.getInstance();
        // Add your atlas server ip address into /etc/hosts as atlas.example.com
        atlasClient.initialize(true, new String[]{"http://atlas.example.com:21000/"}, "admin", "admin", null);

        atlasVariables = new AtlasVariables();
        final Properties atlasProperties = new Properties();
        try (InputStream in = ITNiFiFlowAnalyzer.class.getResourceAsStream("/atlas-application.properties")) {
            atlasProperties.load(in);
            atlasVariables.setAtlasProperties(atlasProperties);
        }

    }

    @Test
    public void testFetchNiFiFlow() throws Exception {
        final NiFiFlowAnalyzer flowAnalyzer = new NiFiFlowAnalyzer(nifiClient);
        final NiFiFlow niFiFlow = flowAnalyzer.analyzeProcessGroup(atlasVariables);
        niFiFlow.dump();

        final List<NiFiFlowPath> niFiFlowPaths = flowAnalyzer.analyzePaths(niFiFlow);
        niFiFlowPaths.forEach(path -> logger.info("{} -> {} -> {}",
                path.getIncomingPaths().size(),
                niFiFlow.getProcessors().get(path.getId()).getName(),
                path.getOutgoingPaths().size()));
    }

    @Test
    public void testRegisterNiFiFlow() throws Exception {
        final NiFiFlowAnalyzer flowAnalyzer = new NiFiFlowAnalyzer(nifiClient);
        final NiFiFlow niFiFlow = flowAnalyzer.analyzeProcessGroup(atlasVariables);
        niFiFlow.dump();

        final List<NiFiFlowPath> niFiFlowPaths = flowAnalyzer.analyzePaths(niFiFlow);
        logger.info("nifiFlowPath={}", niFiFlowPaths);

        atlasClient.registerNiFiFlow(niFiFlow, niFiFlowPaths);
    }

}
