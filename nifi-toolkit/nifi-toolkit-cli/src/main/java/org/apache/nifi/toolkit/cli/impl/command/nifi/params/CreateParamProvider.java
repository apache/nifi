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
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.StringResult;
import org.apache.nifi.toolkit.client.ControllerClient;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ParameterProviderDTO;
import org.apache.nifi.web.api.entity.ParameterProviderEntity;

import java.io.IOException;
import java.util.Properties;

public class CreateParamProvider extends AbstractNiFiCommand<StringResult> {

    public CreateParamProvider() {
        super("create-param-provider", StringResult.class);
    }

    @Override
    public String getDescription() {
        return "Creates a parameter provider. " +
                "After creating the parameter provider, its properties can be configured using the set-param-provider-property command.";
    }

    @Override
    protected void doInitialize(Context context) {
        super.doInitialize(context);
        addOption(CommandOption.PARAM_PROVIDER_NAME.createOption());
        addOption(CommandOption.PARAM_PROVIDER_TYPE.createOption());
        addOption(CommandOption.PARAM_PROVIDER_GROUP_ID.createOption());
        addOption(CommandOption.PARAM_PROVIDER_ARTIFACT_ID.createOption());
        addOption(CommandOption.PARAM_PROVIDER_VERSION.createOption());
    }

    @Override
    public StringResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {

        final String paramProviderName = getRequiredArg(properties, CommandOption.PARAM_PROVIDER_NAME);
        final String paramProviderType = getRequiredArg(properties, CommandOption.PARAM_PROVIDER_TYPE);
        final String bundleGroupId = getRequiredArg(properties, CommandOption.PARAM_PROVIDER_GROUP_ID);
        final String bundleArtifactId = getRequiredArg(properties, CommandOption.PARAM_PROVIDER_ARTIFACT_ID);
        final String bundleVersion = getRequiredArg(properties, CommandOption.PARAM_PROVIDER_VERSION);

        final ParameterProviderDTO paramProviderDTO = new ParameterProviderDTO();
        paramProviderDTO.setName(paramProviderName);
        paramProviderDTO.setType(paramProviderType);

        final BundleDTO bundle = new BundleDTO();
        bundle.setArtifact(bundleArtifactId);
        bundle.setGroup(bundleGroupId);
        bundle.setVersion(bundleVersion);

        paramProviderDTO.setBundle(bundle);

        final ParameterProviderEntity paramProviderEntity = new ParameterProviderEntity();
        paramProviderEntity.setComponent(paramProviderDTO);
        paramProviderEntity.setRevision(getInitialRevisionDTO());

        final ControllerClient controllerClient = client.getControllerClient();
        final ParameterProviderEntity createdParamProvider = controllerClient.createParamProvider(paramProviderEntity);
        return new StringResult(createdParamProvider.getId(), isInteractive());
    }
}
