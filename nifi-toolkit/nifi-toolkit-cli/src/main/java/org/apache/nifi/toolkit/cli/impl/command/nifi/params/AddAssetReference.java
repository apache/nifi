/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.toolkit.cli.impl.command.nifi.params;

import org.apache.commons.cli.MissingOptionException;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.result.VoidResult;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.toolkit.client.ParamContextClient;
import org.apache.nifi.web.api.dto.AssetReferenceDTO;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterDTO;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextUpdateRequestEntity;
import org.apache.nifi.web.api.entity.ParameterEntity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

public class AddAssetReference extends AbstractUpdateParamContextCommand<VoidResult> {

    public AddAssetReference() {
        super("add-asset-reference", VoidResult.class);
    }

    @Override
    public String getDescription() {
        return "Adds an asset reference to a parameter. The parameter will be created if it does not already exist.";
    }

    @Override
    protected void doInitialize(Context context) {
        super.doInitialize(context);
        addOption(CommandOption.PARAM_CONTEXT_ID.createOption());
        addOption(CommandOption.PARAM_NAME.createOption());
        addOption(CommandOption.PARAM_DESC.createOption());
        addOption(CommandOption.ASSET_ID.createOption());
        addOption(CommandOption.UPDATE_TIMEOUT.createOption());
    }

    @Override
    public VoidResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {

        final String paramContextId = getRequiredArg(properties, CommandOption.PARAM_CONTEXT_ID);
        final String paramName = getRequiredArg(properties, CommandOption.PARAM_NAME);
        final String paramDescription = getArg(properties, CommandOption.PARAM_DESC);
        final String assetId = getRequiredArg(properties, CommandOption.ASSET_ID);
        final int updateTimeout = getUpdateTimeout(properties);

        // Ensure the context exists...
        final ParamContextClient paramContextClient = client.getParamContextClient();
        final ParameterContextEntity existingParameterContextEntity = paramContextClient.getParamContext(paramContextId, false);
        final ParameterContextDTO existingParameterContextDTO = existingParameterContextEntity.getComponent();

        // Determine if this is an existing param or a new one...
        final Optional<ParameterDTO> existingParam = existingParameterContextDTO.getParameters()
                .stream()
                .map(ParameterEntity::getParameter)
                .filter(p -> p.getName().equals(paramName))
                .findFirst();

        // Construct the DTOs and entities for submitting the update
        final ParameterDTO parameterDTO = existingParam.orElseGet(ParameterDTO::new);
        parameterDTO.setName(paramName);
        parameterDTO.setSensitive(false);
        parameterDTO.setProvided(false);
        parameterDTO.setValue(null);

        if (paramDescription != null) {
            parameterDTO.setDescription(paramDescription);
        }

        if (parameterDTO.getReferencedAssets() == null) {
            parameterDTO.setReferencedAssets(new ArrayList<>());
        }

        final Set<AssetReferenceDTO> assetReferences = new HashSet<>(parameterDTO.getReferencedAssets());
        assetReferences.add(new AssetReferenceDTO(assetId));
        parameterDTO.getReferencedAssets().clear();
        parameterDTO.getReferencedAssets().addAll(assetReferences);

        // Submit the update request...
        final ParameterContextEntity updatedParameterContextEntity = createContextEntityForUpdate(paramContextId, parameterDTO,
                existingParameterContextDTO.getInheritedParameterContexts(), existingParameterContextEntity.getRevision());

        final ParameterContextUpdateRequestEntity updateRequestEntity = paramContextClient.updateParamContext(updatedParameterContextEntity);
        performUpdate(paramContextClient, updatedParameterContextEntity, updateRequestEntity, updateTimeout);

        if (isInteractive()) {
            println();
        }

        return VoidResult.getInstance();
    }
}
