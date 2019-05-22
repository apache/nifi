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
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.FlowClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.nifi.ControllerServicesResult;
import org.apache.nifi.web.api.entity.ControllerServicesEntity;

import java.io.IOException;
import java.util.Properties;

/**
 * Command to get the list of controller services for the reporting tasks.
 */
public class GetControllerServices extends AbstractNiFiCommand<ControllerServicesResult> {

    public GetControllerServices() {
        super("get-services", ControllerServicesResult.class);
    }

    @Override
    public String getDescription() {
        return "Retrieves the list of controller services for the reporting tasks.";
    }

    @Override
    public ControllerServicesResult doExecute(NiFiClient client, Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {
        final FlowClient flowClient = client.getFlowClient();
        final ControllerServicesEntity servicesEntity = flowClient.getControllerServices();
        return new ControllerServicesResult(getResultType(properties), servicesEntity);
    }
}
