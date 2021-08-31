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
import org.apache.nifi.toolkit.cli.impl.client.nifi.ParamContextClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ParamProviderClient;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.result.VoidResult;
import org.apache.nifi.web.api.dto.ComponentReferenceDTO;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.entity.ComponentReferenceEntity;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextUpdateRequestEntity;

import java.io.IOException;
import java.util.Properties;

public class SetParamProviderReference extends AbstractUpdateParamContextCommand<VoidResult> {

    public SetParamProviderReference() {
        super("set-param-provider-reference", VoidResult.class);
    }

    @Override
    public String getDescription() {
        return "Sets a parameter provider on a parameter context";
    }

    @Override
    protected void doInitialize(Context context) {
        super.doInitialize(context);
        addOption(CommandOption.PARAM_CONTEXT_ID.createOption());
        addOption(CommandOption.PARAM_PROVIDER_ID.createOption());
        addOption(CommandOption.PARAM_PROVIDER_SENSITIVE.createOption());
    }

    @Override
    public VoidResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {

        // Required args...
        final String paramContextId = getRequiredArg(properties, CommandOption.PARAM_CONTEXT_ID);
        final String paramProviderId = getRequiredArg(properties, CommandOption.PARAM_PROVIDER_ID);
        final boolean sensitive = Boolean.valueOf(getRequiredArg(properties, CommandOption.PARAM_PROVIDER_SENSITIVE));

        // Ensure the context exists
        final ParamContextClient paramContextClient = client.getParamContextClient();
        final ParameterContextEntity existingParameterContextEntity = paramContextClient.getParamContext(paramContextId, false);
        final ParameterContextDTO existingContextDTO = existingParameterContextEntity.getComponent();

        // Ensure the provider exists
        final ParamProviderClient paramProviderClient = client.getParamProviderClient();
        paramProviderClient.getParamProvider(paramProviderId);

        // Populate the DTO from the existing one
        final ParameterContextDTO parameterContextDTO = new ParameterContextDTO();
        parameterContextDTO.setId(existingParameterContextEntity.getId());
        parameterContextDTO.setParameters(existingContextDTO.getParameters());
        parameterContextDTO.setName(existingContextDTO.getName());
        parameterContextDTO.setInheritedParameterContexts(existingContextDTO.getInheritedParameterContexts());
        parameterContextDTO.setSensitiveParameterProviderRef(existingContextDTO.getSensitiveParameterProviderRef());
        parameterContextDTO.setNonSensitiveParameterProviderRef(existingContextDTO.getNonSensitiveParameterProviderRef());

        // Now make the update
        final ComponentReferenceEntity newProviderReference = new ComponentReferenceEntity();
        newProviderReference.setId(paramProviderId);
        final ComponentReferenceDTO referenceDTO = new ComponentReferenceDTO();
        referenceDTO.setId(paramProviderId);
        newProviderReference.setComponent(referenceDTO);
        if (sensitive) {
            parameterContextDTO.setSensitiveParameterProviderRef(newProviderReference);
        } else {
            parameterContextDTO.setNonSensitiveParameterProviderRef(newProviderReference);
        }

        final ParameterContextEntity updatedParameterContextEntity = new ParameterContextEntity();
        updatedParameterContextEntity.setId(paramContextId);
        updatedParameterContextEntity.setComponent(parameterContextDTO);
        updatedParameterContextEntity.setRevision(existingParameterContextEntity.getRevision());

        // Submit the update request...
        final ParameterContextUpdateRequestEntity updateRequestEntity = paramContextClient.updateParamContext(updatedParameterContextEntity);
        performUpdate(paramContextClient, updatedParameterContextEntity, updateRequestEntity);

        if (isInteractive()) {
            println();
        }

        return VoidResult.getInstance();
    }
}
