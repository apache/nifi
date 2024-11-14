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
package org.apache.nifi.toolkit.cli.impl.command.nifi.params;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.MissingOptionException;
import org.apache.nifi.parameter.ParameterSensitivity;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.nifi.ParamProviderResult;
import org.apache.nifi.toolkit.cli.impl.util.JacksonUtils;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.toolkit.client.ParamContextClient;
import org.apache.nifi.toolkit.client.ParamProviderClient;
import org.apache.nifi.web.api.entity.ParameterContextsEntity;
import org.apache.nifi.web.api.entity.ParameterProviderApplyParametersRequestEntity;
import org.apache.nifi.web.api.entity.ParameterProviderEntity;
import org.apache.nifi.web.api.entity.ParameterProviderParameterApplicationEntity;
import org.apache.nifi.web.api.entity.ParameterProviderParameterFetchEntity;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;

public class FetchParams extends AbstractNiFiCommand<ParamProviderResult> {

    public FetchParams() {
        super("fetch-params", ParamProviderResult.class);
    }

    @Override
    public String getDescription() {
        return "Fetches the parameters of a parameter provider.  If indicated, also applies the parameters to any referencing parameter contexts. " +
                "When applying parameters, the --inputSource (-i) option specifies the location of a JSON file containing a parameterProviderParameterApplication " +
                "entity, which allows detailed configuration of the applied groups and parameters.  For a simpler approach, this argument may be omitted, and " +
                "all fetched groups will be mapped to parameter contexts of the same names, creating new parameter contexts if needed.  To select sensitive vs. non-sensitive parameters, the " +
                "--sensitiveParamPattern (-spp) can be used.  If this is not supplied, all fetched parameters will default to sensitive.  Note that the " +
                "--inputSource argument overrides any parameter sensitivity specified in the --sensitiveParamPattern argument.";
    }

    @Override
    protected void doInitialize(Context context) {
        super.doInitialize(context);
        addOption(CommandOption.PARAM_PROVIDER_ID.createOption());
        addOption(CommandOption.APPLY_PARAMETERS.createOption());
        addOption(CommandOption.INPUT_SOURCE.createOption());
        addOption(CommandOption.SENSITIVE_PARAM_PATTERN.createOption());
    }

    @Override
    public ParamProviderResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {

        final String paramProviderId = getRequiredArg(properties, CommandOption.PARAM_PROVIDER_ID);
        final String sensitiveParamPattern = getArg(properties, CommandOption.SENSITIVE_PARAM_PATTERN);
        final boolean apply = hasArg(properties, CommandOption.APPLY_PARAMETERS);

        // read the content of the input source into memory
        final String inputSource = getArg(properties, CommandOption.INPUT_SOURCE);
        ParameterProviderParameterApplicationEntity parameterApplicationEntity = null;
        if (inputSource != null) {
            final String parameterApplicationJson = getInputSourceContent(inputSource);
            // unmarshall the content into the DTO object
            final ObjectMapper objectMapper = JacksonUtils.getObjectMapper();
            parameterApplicationEntity = objectMapper.readValue(parameterApplicationJson, ParameterProviderParameterApplicationEntity.class);
        }
        final ParamProviderClient paramProviderClient = client.getParamProviderClient();
        final ParamContextClient paramContextClient = client.getParamContextClient();
        final ParameterContextsEntity paramContextEntity = paramContextClient.getParamContexts();
        final ParameterProviderEntity existingParameterProvider = paramProviderClient.getParamProvider(paramProviderId);

        final ParameterProviderParameterFetchEntity fetchEntity = new ParameterProviderParameterFetchEntity();
        fetchEntity.setRevision(existingParameterProvider.getRevision());
        fetchEntity.setId(existingParameterProvider.getId());
        final ParameterProviderEntity fetchedParameterProvider = paramProviderClient.fetchParameters(fetchEntity);

        if (apply) {
            applyParametersAndWait(paramProviderClient, fetchedParameterProvider, parameterApplicationEntity, sensitiveParamPattern);
        }

        return new ParamProviderResult(getResultType(properties), fetchedParameterProvider, paramContextEntity);
    }

    private void applyParametersAndWait(final ParamProviderClient paramProviderClient, final ParameterProviderEntity fetchedParameterProvider,
            final ParameterProviderParameterApplicationEntity inputApplicationEntity, final String sensitiveParamPattern)
            throws NiFiClientException, IOException {
        ParameterProviderParameterApplicationEntity applicationEntity = inputApplicationEntity;
        if (applicationEntity == null) {
            applicationEntity = new ParameterProviderParameterApplicationEntity();
            applicationEntity.setRevision(fetchedParameterProvider.getRevision());
            applicationEntity.setParameterGroupConfigurations(fetchedParameterProvider.getComponent().getParameterGroupConfigurations());
            applicationEntity.getParameterGroupConfigurations().forEach(groupConfiguration -> {
                if (groupConfiguration.getParameterSensitivities() == null) {
                    groupConfiguration.setParameterSensitivities(new HashMap<>());
                }
                groupConfiguration.setSynchronized(true);
                final Set<String> paramNames = groupConfiguration.getParameterSensitivities().keySet();
                paramNames.forEach(paramName -> {
                    ParameterSensitivity sensitivity = ParameterSensitivity.SENSITIVE;
                    if (sensitiveParamPattern != null) {
                        sensitivity = paramName.matches(sensitiveParamPattern) ? ParameterSensitivity.SENSITIVE : ParameterSensitivity.NON_SENSITIVE;
                    }
                    groupConfiguration.getParameterSensitivities().put(paramName, sensitivity);
                });
            });
            applicationEntity.setId(fetchedParameterProvider.getId());
        }
        final ParameterProviderApplyParametersRequestEntity request = paramProviderClient.applyParameters(applicationEntity);

        while (true) {
            final String providerId = fetchedParameterProvider.getId();
            final String requestId = request.getRequest().getRequestId();
            final ParameterProviderApplyParametersRequestEntity entity = paramProviderClient
                    .getParamProviderApplyParametersRequest(providerId, requestId);
            if (entity.getRequest().isComplete()) {
                paramProviderClient.deleteParamProviderApplyParametersRequest(providerId, requestId);
                if (entity.getRequest().getFailureReason() == null) {
                    return;
                }

                throw new RuntimeException("Parameter Provider Application failed: " + entity.getRequest().getFailureReason());
            }

            try {
                Thread.sleep(100L);
            } catch (final InterruptedException e) {
                throw new RuntimeException("Parameter Application interrupted", e);
            }
        }
    }
}
