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
package org.apache.nifi.web.dao;

import org.apache.nifi.asset.Asset;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.components.connector.ConnectorUpdateContext;
import org.apache.nifi.web.api.dto.ConfigurationStepConfigurationDTO;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

public interface ConnectorDAO {

    boolean hasConnector(String id);

    ConnectorNode getConnector(String id);

    List<ConnectorNode> getConnectors();

    ConnectorNode createConnector(String type, String id, BundleCoordinate bundleCoordinate, boolean firstTimeAdded, boolean registerLogObserver);

    void deleteConnector(String id);

    void startConnector(String id);

    void stopConnector(String id);

    void updateConnectorConfigurationStep(String id, String configurationStepName, ConfigurationStepConfigurationDTO configurationStepConfiguration);

    void applyConnectorUpdate(String id, ConnectorUpdateContext updateContext);

    void discardWorkingConfiguration(String id);

    void verifyCanVerifyConfigurationStep(String id, String configurationStepName);

    List<ConfigVerificationResult> verifyConfigurationStep(String id, String configurationStepName, ConfigurationStepConfigurationDTO configurationStepConfiguration);

    List<AllowableValue> fetchAllowableValues(String id, String stepName, String propertyName, String filter);

    void verifyCreateAsset(String id);

    Asset createAsset(String id, String assetId, String assetName, InputStream content) throws IOException;

    List<Asset> getAssets(String id);

    Optional<Asset> getAsset(String assetId);

}

