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
package org.apache.nifi.tests.system.nar;

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class NarProviderAndAutoLoaderIT extends NiFiSystemIT {

    private static final String ROOT_GROUP_ID = "root";

    private static final String CLASSLOADER_INFO_SERVICE_NAR_ARTIFACT = "nifi-nar-provider-service-nar";
    private static final String CLASSLOADER_INFO_SERVICE_CLASS_NAME = "org.apache.nifi.nar.provider.StandardClassLoaderInfoService";

    private static final String CLASSLOADER_INFO_PROCESSOR_NAR_ARTIFACT = "nifi-nar-provider-processors-nar";
    private static final String CLASSLOADER_INFO_PROCESSOR_CLASS_NAME = "org.apache.nifi.nar.provider.GetClassLoaderInfo";

    private static final String EXPECTED_PARENT_CLASSLOADER = "nifi-nar-provider-service-api-nar-%s";

    @Override
    protected Map<String, String> getNifiPropertiesOverrides() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("nifi.nar.library.provider.local-files.implementation", "org.apache.nifi.nar.provider.LocalDirectoryNarProvider");
        properties.put("nifi.nar.library.provider.local-files.source.dir", "./nifi-nar-provider-nars");
        return properties;
    }

    @Override
    protected boolean isDestroyEnvironmentAfterEachTest() {
        return true;
    }

    @Test
    public void testProviedNarsLoadedAndLinkedCorrectly() throws IOException, NiFiClientException, InterruptedException {
        final ControllerServiceEntity classLoaderInfoService = getClientUtil().createControllerService(CLASSLOADER_INFO_SERVICE_CLASS_NAME,
                ROOT_GROUP_ID, NIFI_GROUP_ID, CLASSLOADER_INFO_SERVICE_NAR_ARTIFACT, getNiFiVersion());

        getClientUtil().enableControllerService(classLoaderInfoService);
        getClientUtil().waitForControllerSerivcesEnabled(classLoaderInfoService.getParentGroupId(), classLoaderInfoService.getId());

        final ProcessorEntity getClassLoaderInfo = getClientUtil().createProcessor(CLASSLOADER_INFO_PROCESSOR_CLASS_NAME,
                NIFI_GROUP_ID, CLASSLOADER_INFO_PROCESSOR_NAR_ARTIFACT, getNiFiVersion());

        final Map<String, String> processorPropertiesForUpdate = new HashMap<>();
        processorPropertiesForUpdate.put("ClassLoader Info Service", classLoaderInfoService.getId());
        final ProcessorEntity updatedGetClassLoaderInfo = getClientUtil().updateProcessorProperties(getClassLoaderInfo, processorPropertiesForUpdate);

        final ProcessorEntity terminateFlowFile = getClientUtil().createProcessor("TerminateFlowFile");
        final ConnectionEntity connection = getClientUtil().createConnection(updatedGetClassLoaderInfo, terminateFlowFile, "success");

        getNifiClient().getProcessorClient().runProcessorOnce(updatedGetClassLoaderInfo);
        waitForQueueCount(connection.getId(), 1);

        final String flowFileContent = getClientUtil().getFlowFileContentAsUtf8(connection.getId(), 0);
        final String expectedParentClassLoader = String.format(EXPECTED_PARENT_CLASSLOADER, getNiFiVersion());
        Assertions.assertTrue(flowFileContent.contains(expectedParentClassLoader));
    }

}
