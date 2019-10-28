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
package org.apache.nifi.controller.status.analytics;

import static org.junit.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.Tuple;
import org.junit.Before;
import org.junit.Test;

public class TestStatusAnalyticsModelMapFactory {

    protected NiFiProperties nifiProperties;
    protected ExtensionManager extensionManager;

    @Before
    public void setup() {
        final Map<String, String> otherProps = new HashMap<>();
        final String propsFile = "src/test/resources/conf/nifi.properties";
        nifiProperties = NiFiProperties.createBasicNiFiProperties(propsFile, otherProps);

        // use the system bundle
        Bundle systemBundle = SystemBundle.create(nifiProperties);
        extensionManager = new StandardExtensionDiscoveringManager();
        ((StandardExtensionDiscoveringManager) extensionManager).discoverExtensions(systemBundle, Collections.emptySet());
    }

    @Test
    public void getConnectionStatusModelMap() {
        StatusAnalyticsModelMapFactory factory = new StatusAnalyticsModelMapFactory(extensionManager,nifiProperties);
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = factory.getConnectionStatusModelMap();
        assertNotNull(modelMap.get("queuedCount"));
        assertNotNull(modelMap.get("queuedBytes"));
        StatusAnalyticsModel countModel = modelMap.get("queuedCount").getKey();
        StatusAnalyticsModel bytesModel = modelMap.get("queuedBytes").getKey();
        assertNotNull(countModel);
        assertNotNull(bytesModel);
        assertEquals(countModel.getClass().getName(),"org.apache.nifi.controller.status.analytics.models.OrdinaryLeastSquares");
        assertEquals(bytesModel.getClass().getName(),"org.apache.nifi.controller.status.analytics.models.OrdinaryLeastSquares");
    }
}
