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
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.result.VoidResult;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterDTO;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextUpdateRequestEntity;
import org.apache.nifi.web.api.entity.ParameterEntity;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;

public class DeleteParam extends AbstractUpdateParamContextCommand<VoidResult> {

    public DeleteParam() {
        super("delete-param", VoidResult.class);
    }

    @Override
    public String getDescription() {
        return "Deletes a given parameter from the given parameter context.";
    }

    @Override
    protected void doInitialize(Context context) {
        super.doInitialize(context);
        addOption(CommandOption.PARAM_CONTEXT_ID.createOption());
        addOption(CommandOption.PARAM_NAME.createOption());
    }

    @Override
    public VoidResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {

        // Required args...
        final String paramContextId = getRequiredArg(properties, CommandOption.PARAM_CONTEXT_ID);
        final String paramName = getRequiredArg(properties, CommandOption.PARAM_NAME);

        // Ensure the context exists...
        final ParamContextClient paramContextClient = client.getParamContextClient();
        final ParameterContextEntity existingEntity = paramContextClient.getParamContext(paramContextId);

        // Determine if this is an existing param or a new one...
        final Optional<ParameterDTO> existingParam = existingEntity.getComponent().getParameters().stream()
                .map(p -> p.getParameter())
                .filter(p -> p.getName().equals(paramName))
                .findFirst();

        if (!existingParam.isPresent()) {
            throw new NiFiClientException("Unable to delete parameter, no parameter found with name '" + paramName + "'");
        }

        // Construct the objects for the update, a NULL value indicates to the server to removes the parameter...
        final ParameterDTO parameterDTO = existingParam.get();
        parameterDTO.setValue(null);
        parameterDTO.setDescription(null);
        parameterDTO.setSensitive(null);

        final ParameterEntity parameterEntity = new ParameterEntity();
        parameterEntity.setParameter(parameterDTO);

        final ParameterContextDTO parameterContextDTO = new ParameterContextDTO();
        parameterContextDTO.setId(existingEntity.getId());
        parameterContextDTO.setParameters(Collections.singleton(parameterEntity));

        final ParameterContextEntity updatedParameterContextEntity = new ParameterContextEntity();
        updatedParameterContextEntity.setId(paramContextId);
        updatedParameterContextEntity.setComponent(parameterContextDTO);
        updatedParameterContextEntity.setRevision(existingEntity.getRevision());

        // Submit the update request...
        final ParameterContextUpdateRequestEntity updateRequestEntity = paramContextClient.updateParamContext(updatedParameterContextEntity);
        performUpdate(paramContextClient, updatedParameterContextEntity, updateRequestEntity);

        if (isInteractive()) {
            println();
        }

        return VoidResult.getInstance();
    }
}
