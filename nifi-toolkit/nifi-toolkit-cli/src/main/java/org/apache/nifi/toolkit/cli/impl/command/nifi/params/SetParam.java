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
import org.apache.commons.lang3.StringUtils;
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

public class SetParam extends AbstractUpdateParamContextCommand<VoidResult> {

    public SetParam() {
        super("set-param", VoidResult.class);
    }

    @Override
    public String getDescription() {
        return "Creates or updates a parameter in the given parameter context.";
    }

    @Override
    protected void doInitialize(Context context) {
        super.doInitialize(context);
        addOption(CommandOption.PARAM_CONTEXT_ID.createOption());
        addOption(CommandOption.PARAM_NAME.createOption());
        addOption(CommandOption.PARAM_DESC.createOption());
        addOption(CommandOption.PARAM_VALUE.createOption());
        addOption(CommandOption.PARAM_SENSITIVE.createOption());
    }

    @Override
    public VoidResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {

        // Required args...
        final String paramContextId = getRequiredArg(properties, CommandOption.PARAM_CONTEXT_ID);
        final String paramName = getRequiredArg(properties, CommandOption.PARAM_NAME);

        // Optional args...
        final String paramValue = getArg(properties, CommandOption.PARAM_VALUE);
        final String paramDesc = getArg(properties, CommandOption.PARAM_DESC);
        final String paramSensitive = getArg(properties, CommandOption.PARAM_SENSITIVE);
        if (!StringUtils.isBlank(paramSensitive) && !"true".equals(paramSensitive) && !"false".equals(paramSensitive)) {
            throw new IllegalArgumentException("Parameter sensitive flag must be one of 'true' or 'false'");
        }

        // Ensure the context exists...
        final ParamContextClient paramContextClient = client.getParamContextClient();
        final ParameterContextEntity existingParameterContextEntity = paramContextClient.getParamContext(paramContextId);
        final ParameterContextDTO existingParameterContextDTO = existingParameterContextEntity.getComponent();

        // Determine if this is an existing param or a new one...
        final Optional<ParameterDTO> existingParam = existingParameterContextDTO.getParameters().stream()
                .map(p -> p.getParameter())
                .filter(p -> p.getName().equals(paramName))
                .findFirst();

        if (!existingParam.isPresent() && paramValue == null) {
            throw new IllegalArgumentException("A parameter value is required when creating a new parameter");
        }

        // Construct the objects for the update...
        final ParameterDTO parameterDTO = existingParam.isPresent() ? existingParam.get() : new ParameterDTO();
        parameterDTO.setName(paramName);

        if (paramValue != null) {
            parameterDTO.setValue(paramValue);
        }

        if (paramDesc != null) {
            parameterDTO.setDescription(paramDesc);
        }

        if (!StringUtils.isBlank(paramSensitive)) {
            parameterDTO.setSensitive(Boolean.valueOf(paramSensitive));
        }

        final ParameterEntity parameterEntity = new ParameterEntity();
        parameterEntity.setParameter(parameterDTO);

        final ParameterContextDTO parameterContextDTO = new ParameterContextDTO();
        parameterContextDTO.setId(existingParameterContextEntity.getId());
        parameterContextDTO.setParameters(Collections.singleton(parameterEntity));

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
