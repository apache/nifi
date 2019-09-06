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
package org.apache.nifi.toolkit.cli.impl.command.nifi.pg;

import org.apache.commons.cli.MissingOptionException;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ProcessGroupClient;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.StringResult;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.entity.ParameterContextReferenceEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;

import java.io.IOException;
import java.util.Properties;

public class PGGetParamContext extends AbstractNiFiCommand<StringResult> {

    public PGGetParamContext() {
        super("pg-get-param-context", StringResult.class);
    }

    @Override
    public String getDescription() {
        return "Gets the id of the parameter context that is bound to the given process group.";
    }

    @Override
    protected void doInitialize(Context context) {
        super.doInitialize(context);
        addOption(CommandOption.PG_ID.createOption());
    }

    @Override
    public StringResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {
        final String pgId = getRequiredArg(properties, CommandOption.PG_ID);
        final ProcessGroupClient pgClient = client.getProcessGroupClient();
        final ProcessGroupEntity pgEntity = pgClient.getProcessGroup(pgId);

        final String paramContextId = getParamContextId(pgEntity.getComponent());
        return new StringResult(paramContextId, isInteractive());
    }

    private String getParamContextId(final ProcessGroupDTO processGroup) {
        if (processGroup == null) {
            return "";
        }

        final ParameterContextReferenceEntity parameterContextReference = processGroup.getParameterContext();
        if (parameterContextReference == null) {
            return "";
        }

        final String paramContextId = parameterContextReference.getId();
        if (StringUtils.isBlank(paramContextId)) {
            return "";
        }

        return paramContextId;
    }
}
