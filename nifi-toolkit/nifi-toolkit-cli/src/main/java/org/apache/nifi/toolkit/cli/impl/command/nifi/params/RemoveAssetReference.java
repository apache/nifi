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
import java.util.List;
import java.util.Properties;

public class RemoveAssetReference extends AbstractUpdateParamContextCommand<VoidResult> {

    public RemoveAssetReference() {
        super("remove-asset-reference", VoidResult.class);
    }

    @Override
    public String getDescription() {
        return "Removes an asset reference from a given parameter.";
    }

    @Override
    protected void doInitialize(Context context) {
        super.doInitialize(context);
        addOption(CommandOption.PARAM_CONTEXT_ID.createOption());
        addOption(CommandOption.PARAM_NAME.createOption());
        addOption(CommandOption.ASSET_ID.createOption());
        addOption(CommandOption.UPDATE_TIMEOUT.createOption());
    }

    @Override
    public VoidResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {

        final String paramContextId = getRequiredArg(properties, CommandOption.PARAM_CONTEXT_ID);
        final String paramName = getRequiredArg(properties, CommandOption.PARAM_NAME);
        final String assetId = getRequiredArg(properties, CommandOption.ASSET_ID);
        final int updateTimeout = getUpdateTimeout(properties);

        // Ensure the context exists...
        final ParamContextClient paramContextClient = client.getParamContextClient();
        final ParameterContextEntity existingParameterContextEntity = paramContextClient.getParamContext(paramContextId, false);
        final ParameterContextDTO existingParameterContextDTO = existingParameterContextEntity.getComponent();

        // Find the existing parameter by name or throw an exception
        final ParameterDTO existingParam = existingParameterContextDTO.getParameters()
                .stream()
                .map(ParameterEntity::getParameter)
                .filter(p -> p.getName().equals(paramName))
                .findFirst()
                .orElseThrow(() -> new NiFiClientException("Parameter does not exist with the given name"));

        // Remove the given assetId from the referenced assets, or throw an exception if
        // not referenced
        final List<AssetReferenceDTO> assetReferences = existingParam.getReferencedAssets();
        if (assetReferences == null) {
            throw new NiFiClientException("Parameter does not reference any assets");
        }

        final AssetReferenceDTO assetReferenceDTO = new AssetReferenceDTO(assetId);
        if (!assetReferences.contains(assetReferenceDTO)) {
            throw new NiFiClientException("Parameter does not reference the given asset");
        }
        assetReferences.remove(assetReferenceDTO);
        existingParam.setValue(null);

        // Submit the update request...
        final ParameterContextEntity updatedParameterContextEntity = createContextEntityForUpdate(paramContextId, existingParam,
                existingParameterContextDTO.getInheritedParameterContexts(), existingParameterContextEntity.getRevision());

        final ParameterContextUpdateRequestEntity updateRequestEntity = paramContextClient.updateParamContext(updatedParameterContextEntity);
        performUpdate(paramContextClient, updatedParameterContextEntity, updateRequestEntity, updateTimeout);

        if (isInteractive()) {
            println();
        }

        return VoidResult.getInstance();
    }

}
