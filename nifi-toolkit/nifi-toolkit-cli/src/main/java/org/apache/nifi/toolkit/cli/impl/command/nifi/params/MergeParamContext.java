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
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ParamContextClient;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.result.VoidResult;
import org.apache.nifi.toolkit.cli.impl.util.JacksonUtils;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterDTO;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextUpdateRequestEntity;
import org.apache.nifi.web.api.entity.ParameterEntity;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

public class MergeParamContext extends AbstractUpdateParamContextCommand<VoidResult> {

    public MergeParamContext() {
        super("merge-param-context", VoidResult.class);
    }

    @Override
    public String getDescription() {
        return "Adds any parameters that exist in the exported context that don't exist in the existing context.";
    }

    @Override
    protected void doInitialize(Context context) {
        super.doInitialize(context);
        addOption(CommandOption.PARAM_CONTEXT_ID.createOption());
        addOption(CommandOption.INPUT_SOURCE.createOption());
    }

    @Override
    public VoidResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {

        final String existingContextId = getRequiredArg(properties, CommandOption.PARAM_CONTEXT_ID);

        // read the content of the input source into memory
        final String inputSource = getRequiredArg(properties, CommandOption.INPUT_SOURCE);
        final String paramContextJson = getInputSourceContent(inputSource);

        // unmarshall the content into the DTO object
        final ObjectMapper objectMapper = JacksonUtils.getObjectMapper();
        final ParameterContextDTO incomingContext = objectMapper.readValue(paramContextJson, ParameterContextDTO.class);
        if (incomingContext.getParameters() == null) {
            incomingContext.setParameters(new LinkedHashSet<>());
        }

        // retrieve the existing context by id
        final ParamContextClient paramContextClient = client.getParamContextClient();
        final ParameterContextEntity existingContextEntity = paramContextClient.getParamContext(existingContextId);

        final ParameterContextDTO existingContext = existingContextEntity.getComponent();
        if (existingContext.getParameters() == null) {
            existingContext.setParameters(new LinkedHashSet<>());
        }

        final Set<ParameterEntity> createdParameters = new LinkedHashSet<>();

        // determine which incoming params are not in the set of existing params
        for (final ParameterEntity incomingParameterEntity : incomingContext.getParameters()) {
            final ParameterDTO incomingParameter = incomingParameterEntity.getParameter();
            final String parameterName = incomingParameter.getName();

            final Optional<ParameterDTO> existingParameter = existingContext.getParameters().stream()
                    .map(p -> p.getParameter())
                    .filter(p -> p.getName().equals(parameterName))
                    .findFirst();

            if (!existingParameter.isPresent()) {
                final ParameterEntity createdParam = createParameter(incomingParameter);
                createdParameters.add(createdParam);
            }
        }

        // create a new entity to issue an update request with the newly added params
        final ParameterContextDTO updatedContextDto = new ParameterContextDTO();
        updatedContextDto.setId(existingContext.getId());
        updatedContextDto.setParameters(createdParameters);

        final ParameterContextEntity updatedContextEntity = new ParameterContextEntity();
        updatedContextEntity.setId(existingContext.getId());
        updatedContextEntity.setComponent(updatedContextDto);
        updatedContextEntity.setRevision(existingContextEntity.getRevision());

        // Submit the update request...
        final ParameterContextUpdateRequestEntity updateRequestEntity = paramContextClient.updateParamContext(updatedContextEntity);
        performUpdate(paramContextClient, updatedContextEntity, updateRequestEntity);

        printlnIfInteractive("");
        return VoidResult.getInstance();
    }

    private ParameterEntity createParameter(final ParameterDTO incomingParam) throws CommandException {
        final String parameterName = incomingParam.getName();
        printlnIfInteractive("Found parameter to add - '" + parameterName + "'");

        final ParameterDTO newParameter = new ParameterDTO();
        newParameter.setName(incomingParam.getName());
        newParameter.setDescription(incomingParam.getDescription());
        newParameter.setSensitive(incomingParam.getSensitive());
        newParameter.setValue(incomingParam.getValue());

        final ParameterEntity newParameterEntity = new ParameterEntity();
        newParameterEntity.setParameter(newParameter);
        return newParameterEntity;
    }

}
