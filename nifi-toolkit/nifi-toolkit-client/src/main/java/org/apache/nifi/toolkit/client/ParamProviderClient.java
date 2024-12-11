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

import org.apache.nifi.web.api.entity.ParameterProviderApplyParametersRequestEntity;
import org.apache.nifi.web.api.entity.ParameterProviderEntity;
import org.apache.nifi.web.api.entity.ParameterProviderParameterApplicationEntity;
import org.apache.nifi.web.api.entity.ParameterProviderParameterFetchEntity;
import org.apache.nifi.web.api.entity.VerifyConfigRequestEntity;

import java.io.IOException;

public interface ParamProviderClient {

    ParameterProviderEntity getParamProvider(String id) throws NiFiClientException, IOException;

    ParameterProviderEntity updateParamProvider(ParameterProviderEntity paramProvider) throws NiFiClientException, IOException;

    ParameterProviderEntity deleteParamProvider(String id, String version) throws NiFiClientException, IOException;

    ParameterProviderEntity deleteParamProvider(String id, String version, boolean disconnectedNodeAcknowledged) throws NiFiClientException, IOException;

    ParameterProviderEntity fetchParameters(ParameterProviderParameterFetchEntity parameterFetchEntity) throws NiFiClientException, IOException;

    ParameterProviderApplyParametersRequestEntity applyParameters(ParameterProviderParameterApplicationEntity parameterApplicationEntity) throws NiFiClientException, IOException;

    ParameterProviderApplyParametersRequestEntity getParamProviderApplyParametersRequest(String providerId, String applyParametersRequestId) throws NiFiClientException, IOException;

    ParameterProviderApplyParametersRequestEntity deleteParamProviderApplyParametersRequest(String providerId, String applyParametersRequestId) throws NiFiClientException, IOException;

    VerifyConfigRequestEntity submitConfigVerificationRequest(VerifyConfigRequestEntity configRequestEntity) throws NiFiClientException, IOException;

    VerifyConfigRequestEntity getConfigVerificationRequest(String providerId, String verificationRequestId) throws NiFiClientException, IOException;

    VerifyConfigRequestEntity deleteConfigVerificationRequest(String providerId, String verificationRequestId) throws NiFiClientException, IOException;

}
