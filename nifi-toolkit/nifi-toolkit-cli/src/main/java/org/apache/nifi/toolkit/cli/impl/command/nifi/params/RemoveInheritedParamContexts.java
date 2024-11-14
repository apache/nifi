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
import org.apache.nifi.toolkit.cli.impl.result.VoidResult;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.toolkit.client.ParamContextClient;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextReferenceEntity;
import org.apache.nifi.web.api.entity.ParameterContextUpdateRequestEntity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class RemoveInheritedParamContexts extends AbstractUpdateParamContextCommand<VoidResult> {

    public RemoveInheritedParamContexts() {
        super("remove-inherited-param-contexts", VoidResult.class);
    }

    @Override
    public String getDescription() {
        return "Removes all inherited parameter contexts from the given parameter context";
    }

    @Override
    protected void doInitialize(Context context) {
        super.doInitialize(context);
        addOption(CommandOption.PARAM_CONTEXT_ID.createOption());
    }

    @Override
    public VoidResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {

        // Required args...
        final String paramContextId = getRequiredArg(properties, CommandOption.PARAM_CONTEXT_ID);

        // Optional args...
        final int updateTimeout = getUpdateTimeout(properties);

        // Ensure the context exists...
        final ParamContextClient paramContextClient = client.getParamContextClient();
        final ParameterContextEntity existingParameterContextEntity = paramContextClient.getParamContext(paramContextId, false);

        final ParameterContextDTO parameterContextDTO = new ParameterContextDTO();
        parameterContextDTO.setId(existingParameterContextEntity.getId());
        parameterContextDTO.setParameters(existingParameterContextEntity.getComponent().getParameters());

        // we need to explicitly set null for the sensitive parameters as we are getting
        // **** for the values
        // on the client side. This is the only way for the server side to know that the
        // values are not changed
        // during the update parameter context request
        parameterContextDTO.getParameters()
                .stream()
                .filter(t -> t.getParameter().getSensitive().booleanValue())
                .forEach(t -> t.getParameter().setValue(null));

        final ParameterContextEntity updatedParameterContextEntity = new ParameterContextEntity();
        updatedParameterContextEntity.setId(paramContextId);
        updatedParameterContextEntity.setComponent(parameterContextDTO);
        updatedParameterContextEntity.setRevision(existingParameterContextEntity.getRevision());

        final List<ParameterContextReferenceEntity> referenceEntities = new ArrayList<>();
        parameterContextDTO.setInheritedParameterContexts(referenceEntities);

        // Submit the update request...
        final ParameterContextUpdateRequestEntity updateRequestEntity = paramContextClient.updateParamContext(updatedParameterContextEntity);
        performUpdate(paramContextClient, updatedParameterContextEntity, updateRequestEntity, updateTimeout);

        if (isInteractive()) {
            println();
        }

        return VoidResult.getInstance();
    }
}
