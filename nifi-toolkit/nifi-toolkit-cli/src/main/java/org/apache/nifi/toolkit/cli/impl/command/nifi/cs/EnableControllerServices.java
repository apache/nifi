/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiActivateCommand;
import org.apache.nifi.toolkit.cli.impl.result.VoidResult;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceRunStatusEntity;
import org.apache.nifi.web.api.entity.ControllerServicesEntity;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Command for enabling controller services for reporting tasks.
 */
public class EnableControllerServices extends AbstractNiFiActivateCommand<ControllerServiceEntity,
        ControllerServiceRunStatusEntity> {

    public EnableControllerServices() {
        super("enable-services");
    }

    @Override
    public String getDescription() {
        return "Attempts to enable all controller services for reporting tasks. In stand-alone mode this command " +
            "will not produce all of the output seen in interactive mode unless the --verbose argument is specified.";
    }

    @Override
    protected void doInitialize(final Context context) {
        addOption(CommandOption.CS_ID.createOption());
    }

    @Override
    public VoidResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {
        final String csId = getArg(properties, CommandOption.CS_ID);
        final Set<ControllerServiceEntity> serviceEntities = new HashSet<>();

        if (StringUtils.isBlank(csId)) {
            final ControllerServicesEntity servicesEntity = client.getFlowClient().getControllerServices();
            serviceEntities.addAll(servicesEntity.getControllerServices());
        } else {
            serviceEntities.add(client.getControllerServicesClient().getControllerService(csId));
        }

        activate(client, properties, serviceEntities, "ENABLED");

        return VoidResult.getInstance();
    }

    @Override
    public ControllerServiceRunStatusEntity getRunStatusEntity() {
        return new ControllerServiceRunStatusEntity();
    }

    @Override
    public ControllerServiceEntity activateComponent(final NiFiClient client,
            final ControllerServiceEntity serviceEntity, final ControllerServiceRunStatusEntity runStatusEntity)
            throws NiFiClientException, IOException {
        return client.getControllerServicesClient().activateControllerService(serviceEntity.getId(), runStatusEntity);
    }

    @Override
    public String getDispName(final ControllerServiceEntity serviceEntity) {
        return "Controller service \"" + serviceEntity.getComponent().getName() + "\" " +
                "(id: " + serviceEntity.getId() + ")";
    }
}
