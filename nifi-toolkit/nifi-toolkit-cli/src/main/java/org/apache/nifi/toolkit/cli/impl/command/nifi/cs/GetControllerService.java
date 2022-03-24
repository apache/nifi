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
package org.apache.nifi.toolkit.cli.impl.command.nifi.cs;

import org.apache.commons.cli.MissingOptionException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ControllerServicesClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.nifi.ControllerServiceResult;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;

import java.io.IOException;
import java.util.Properties;

/**
 * Command for retrieving the status of a controller service.
 */
public class GetControllerService extends AbstractNiFiCommand<ControllerServiceResult> {

    public GetControllerService() {
        super("get-service", ControllerServiceResult.class);
    }

    @Override
    public String getDescription() {
        return "Retrieves the status for a controller service.";
    }

    @Override
    protected void doInitialize(Context context) {
        addOption(CommandOption.CS_ID.createOption());
    }

    @Override
    public ControllerServiceResult doExecute(NiFiClient client, Properties properties) throws NiFiClientException, IOException, MissingOptionException {
        final String csId = getRequiredArg(properties, CommandOption.CS_ID);
        final ControllerServicesClient csClient = client.getControllerServicesClient();

        final ControllerServiceEntity csEntityResult = csClient.getControllerService(csId);
        return new ControllerServiceResult(getResultType(properties), csEntityResult);
    }
}
