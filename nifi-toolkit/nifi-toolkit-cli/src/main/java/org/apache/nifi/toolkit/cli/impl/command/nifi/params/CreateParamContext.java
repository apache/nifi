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
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.StringResult;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.entity.ParameterContextEntity;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class CreateParamContext extends AbstractNiFiCommand<StringResult> {

    public CreateParamContext() {
        super("create-param-context", StringResult.class);
    }

    @Override
    public String getDescription() {
        return "Creates a parameter context with the given name. " +
                "After creating the parameter context, parameters can be added using the set-param command.";
    }

    @Override
    protected void doInitialize(Context context) {
        super.doInitialize(context);
        addOption(CommandOption.PARAM_CONTEXT_NAME.createOption());
        addOption(CommandOption.PARAM_CONTEXT_DESC.createOption());
    }

    @Override
    public StringResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {

        final String paramContextName = getRequiredArg(properties, CommandOption.PARAM_CONTEXT_NAME);
        final String paramContextDesc = getArg(properties, CommandOption.PARAM_CONTEXT_DESC);

        final ParameterContextDTO paramContextDTO = new ParameterContextDTO();
        paramContextDTO.setName(paramContextName);
        paramContextDTO.setParameters(Collections.emptySet());

        if (!StringUtils.isBlank(paramContextDesc)) {
            paramContextDTO.setDescription(paramContextDesc);
        }

        final ParameterContextEntity paramContextEntity = new ParameterContextEntity();
        paramContextEntity.setComponent(paramContextDTO);
        paramContextEntity.setRevision(getInitialRevisionDTO());

        final ParamContextClient paramContextClient = client.getParamContextClient();
        final ParameterContextEntity createdParamContext = paramContextClient.createParamContext(paramContextEntity);
        return new StringResult(createdParamContext.getId(), isInteractive());
    }
}
