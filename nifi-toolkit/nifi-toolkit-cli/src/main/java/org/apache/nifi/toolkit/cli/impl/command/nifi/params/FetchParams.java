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

import org.apache.commons.cli.MissingOptionException;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ParamProviderClient;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.nifi.ParamProviderResult;
import org.apache.nifi.web.api.entity.ParameterProviderApplyParametersRequestEntity;
import org.apache.nifi.web.api.entity.ParameterProviderEntity;
import org.apache.nifi.web.api.entity.ParameterProviderParameterApplicationEntity;
import org.apache.nifi.web.api.entity.ParameterProviderParameterFetchEntity;

import java.io.IOException;
import java.util.Properties;

public class FetchParams extends AbstractNiFiCommand<ParamProviderResult> {

    public FetchParams() {
        super("fetch-params", ParamProviderResult.class);
    }

    @Override
    public String getDescription() {
        return "Fetches the parameters of a parameter provider.  If indicated, also applies the parameters to any referencing parameter contexts.";
    }

    @Override
    protected void doInitialize(Context context) {
        super.doInitialize(context);
        addOption(CommandOption.PARAM_PROVIDER_ID.createOption());
        addOption(CommandOption.APPLY_PARAMETERS.createOption());
    }

    @Override
    public ParamProviderResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {

        final String paramProviderId = getRequiredArg(properties, CommandOption.PARAM_PROVIDER_ID);
        final boolean apply = hasArg(properties, CommandOption.APPLY_PARAMETERS);

        final ParamProviderClient paramProviderClient = client.getParamProviderClient();
        final ParameterProviderEntity existingParameterProvider = paramProviderClient.getParamProvider(paramProviderId);

        final ParameterProviderParameterFetchEntity fetchEntity = new ParameterProviderParameterFetchEntity();
        fetchEntity.setRevision(existingParameterProvider.getRevision());
        fetchEntity.setId(existingParameterProvider.getId());
        final ParameterProviderEntity fetchedParameterProvider = paramProviderClient.fetchParameters(fetchEntity);

        if (apply) {
            applyParametersAndWait(paramProviderClient, fetchedParameterProvider);
        }

        return new ParamProviderResult(getResultType(properties), fetchedParameterProvider);
    }

    private void applyParametersAndWait(final ParamProviderClient paramProviderClient, final ParameterProviderEntity fetchedParameterProvider) throws NiFiClientException, IOException {
        final ParameterProviderParameterApplicationEntity applicationEntity = new ParameterProviderParameterApplicationEntity();
        applicationEntity.setRevision(fetchedParameterProvider.getRevision());
        applicationEntity.setParameterNameGroups(fetchedParameterProvider.getComponent().getFetchedParameterNameGroups());
        applicationEntity.setId(fetchedParameterProvider.getId());
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
