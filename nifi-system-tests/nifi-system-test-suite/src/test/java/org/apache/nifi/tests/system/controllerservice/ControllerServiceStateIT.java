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

package org.apache.nifi.tests.system.controllerservice;

import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ControllerServiceStateIT extends NiFiSystemIT {

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createTwoNodeInstanceFactory();
    }

    @Test
    public void testLocalClusterState() throws NiFiClientException, IOException, InterruptedException {
        final String tempDir = System.getProperty("java.io.tmpdir");

        final File storageDir = new File(tempDir);
        final File localStateFile1 = new File(storageDir, "local-state-1.properties");
        final File clusterStateFile1 = new File(storageDir, "cluster-state-1.properties");
        final File localStateFile2 = new File(storageDir, "local-state-2.properties");
        final File clusterStateFile2 = new File(storageDir, "cluster-state-2.properties");
        localStateFile1.delete();
        clusterStateFile1.delete();
        localStateFile2.delete();
        clusterStateFile2.delete();

        final ControllerServiceEntity failureService = getClientUtil().createControllerService("VerifyLocalClusterStateService");
        getClientUtil().updateControllerServiceProperties(failureService, Map.of("File Storage Location", tempDir));
        getClientUtil().enableControllerService(failureService);

        final ProcessorEntity setState = getClientUtil().createProcessor("SetState");
        getClientUtil().updateProcessorProperties(setState, Map.of("State Service", failureService.getId()));
        getClientUtil().setAutoTerminatedRelationships(setState, "success");
        getClientUtil().waitForValidProcessor(setState.getId());

        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ConnectionEntity connection = getClientUtil().createConnection(generate, setState, "success");

        getClientUtil().startProcessor(generate);
        waitForQueueCount(connection, getNumberOfNodes());

        getClientUtil().startProcessor(setState);
        waitForQueueCount(connection, 0);

        getNiFiInstance().stop();
        getNiFiInstance().start(true);

        waitFor(() -> localStateFile1.exists() && clusterStateFile1.exists());

        final Properties localProperties1 = loadPropertiesFromFile(localStateFile1);
        final Properties clusterProperties1 = loadPropertiesFromFile(clusterStateFile1);
        final Properties localProperties2 = loadPropertiesFromFile(localStateFile2);
        final Properties clusterProperties2 = loadPropertiesFromFile(clusterStateFile2);

        final Properties expectedLocalProperties = new Properties();
        expectedLocalProperties.setProperty("local", "local");
        final Properties expectedClusterProperties = new Properties();
        expectedClusterProperties.setProperty("cluster", "cluster");

        assertEquals(expectedLocalProperties, localProperties1);
        assertEquals(expectedLocalProperties, localProperties2);
        assertEquals(expectedClusterProperties, clusterProperties1);
        assertEquals(expectedClusterProperties, clusterProperties2);
    }

    private Properties loadPropertiesFromFile(final File file) throws IOException {
        final Properties properties = new Properties();
        try (final FileInputStream in = new FileInputStream(file)) {
            properties.load(in);
        }
        return properties;
    }
}
