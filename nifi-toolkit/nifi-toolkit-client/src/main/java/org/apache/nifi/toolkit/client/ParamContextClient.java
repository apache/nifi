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
package org.apache.nifi.toolkit.client;

import org.apache.nifi.web.api.entity.AssetEntity;
import org.apache.nifi.web.api.entity.AssetsEntity;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextUpdateRequestEntity;
import org.apache.nifi.web.api.entity.ParameterContextsEntity;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public interface ParamContextClient {

    ParameterContextsEntity getParamContexts() throws NiFiClientException, IOException;

    ParameterContextEntity getParamContext(String id, boolean includeInheritedParameters) throws NiFiClientException, IOException;

    ParameterContextEntity createParamContext(ParameterContextEntity paramContext) throws NiFiClientException, IOException;

    ParameterContextEntity deleteParamContext(String id, String version) throws NiFiClientException, IOException;

    ParameterContextEntity deleteParamContext(String id, String version, boolean disconnectedNodeAcknowledged) throws NiFiClientException, IOException;

    ParameterContextUpdateRequestEntity updateParamContext(ParameterContextEntity paramContext) throws NiFiClientException, IOException;

    ParameterContextUpdateRequestEntity getParamContextUpdateRequest(String contextId, String updateRequestId) throws NiFiClientException, IOException;

    ParameterContextUpdateRequestEntity deleteParamContextUpdateRequest(String contextId, String updateRequestId) throws NiFiClientException, IOException;

    AssetEntity createAsset(String contextId, String assetName, File file) throws NiFiClientException, IOException;

    AssetsEntity getAssets(String contextId) throws NiFiClientException, IOException;

    Path getAssetContent(String contextId, String assetId, File outputDirectory) throws NiFiClientException, IOException;

    AssetEntity deleteAsset(String contextId, String assetId) throws NiFiClientException, IOException;

    AssetEntity deleteAsset(String contextId, String assetId, boolean disconnectedNodeAcknowledged) throws NiFiClientException, IOException;
}
