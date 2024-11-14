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
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.toolkit.client.ParamProviderClient;
import org.apache.nifi.web.api.entity.ParameterProviderEntity;

import java.io.IOException;
import java.util.Properties;

public class DeleteParamProvider extends AbstractNiFiCommand<StringResult> {

    public DeleteParamProvider() {
        super("delete-param-provider", StringResult.class);
    }

    @Override
    public String getDescription() {
        return "Deletes a parameter provider.";
    }

    @Override
    protected void doInitialize(Context context) {
        super.doInitialize(context);
        addOption(CommandOption.PARAM_PROVIDER_ID.createOption());
    }

    @Override
    public StringResult doExecute(NiFiClient client, Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {
        final String paramProviderId = getRequiredArg(properties, CommandOption.PARAM_PROVIDER_ID);

        final ParamProviderClient paramProviderClient = client.getParamProviderClient();
        final ParameterProviderEntity existingParamContext = paramProviderClient.getParamProvider(paramProviderId);

        final String version = String.valueOf(existingParamContext.getRevision().getVersion());
        paramProviderClient.deleteParamProvider(paramProviderId, version);
        return new StringResult(paramProviderId, isInteractive());
    }

}
