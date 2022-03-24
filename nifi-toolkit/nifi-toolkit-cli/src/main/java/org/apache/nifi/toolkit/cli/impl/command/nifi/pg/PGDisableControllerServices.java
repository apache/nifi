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
import org.apache.nifi.toolkit.cli.impl.client.nifi.FlowClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.cs.ControllerServiceStateCounts;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.cs.ControllerServiceStates;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.cs.ControllerServiceUtil;
import org.apache.nifi.toolkit.cli.impl.result.VoidResult;
import org.apache.nifi.web.api.entity.ActivateControllerServicesEntity;
import org.apache.nifi.web.api.entity.BulletinEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServicesEntity;

import java.io.IOException;
import java.util.Properties;

public class PGDisableControllerServices extends AbstractNiFiCommand<VoidResult> {

    public static final int MAX_DISABLING_ITERATIONS = 20;
    public static final long DELAY_MS = 2000;

    public PGDisableControllerServices() {
        super("pg-disable-services", VoidResult.class);
    }

    @Override
    public String getDescription() {
        return "Disables the controller services in the given process group. Any services that are in use by a running component " +
                "will fail to be disabled and will need to be stopped first using pg-stop.";
    }

    @Override
    protected void doInitialize(final Context context) {
        addOption(CommandOption.PG_ID.createOption());
    }

    @Override
    public VoidResult doExecute(NiFiClient client, Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {

        final FlowClient flowClient = client.getFlowClient();
        final String pgId = getRequiredArg(properties, CommandOption.PG_ID);

        final ControllerServiceStateCounts initialServiceStates = getControllerServiceStates(flowClient, pgId);

        // if we have no enabled or enabling services then there is nothing to do so return
        if (initialServiceStates.getEnabled() == 0 && initialServiceStates.getEnabling() == 0) {
            if (shouldPrint(properties)) {
                println();
                println("No services are currently enabled/enabling, nothing to do...");
                println();
            }
            return VoidResult.getInstance();
        }

        if (shouldPrint(properties)) {
            println();
            println("Starting states:");
            printControllerServiceStates(initialServiceStates);
            println();
            println("Attempting to disable services...");
        }

        // send the request to disable services and wait a second
        final ActivateControllerServicesEntity disableEntity = new ActivateControllerServicesEntity();
        disableEntity.setId(pgId);
        disableEntity.setState(ActivateControllerServicesEntity.STATE_DISABLED);

        flowClient.activateControllerServices(disableEntity);
        sleep(1000);

        // wait for disabling services to become disabled, or until we've waited up to max number of waits
        int disablingWaitCount = 1;
        ControllerServiceStateCounts serviceStates = getControllerServiceStates(flowClient, pgId);
        while (serviceStates.getDisabling() > 0 && disablingWaitCount < MAX_DISABLING_ITERATIONS) {
            if (shouldPrint(properties)) {
                println("Currently " + serviceStates.getDisabling() + " services are disabling, waiting to finish before proceeding ("
                        + disablingWaitCount + " of " + MAX_DISABLING_ITERATIONS + ")...");
            }
            sleep(DELAY_MS);
            disablingWaitCount++;
            serviceStates = getControllerServiceStates(flowClient, pgId);
        }

        // if we have still have disabling services then we may have a stuck service so throw an exception
        if (serviceStates.getDisabling() > 0) {
            if (shouldPrint(properties)) {
                printServicesStillDisabling(flowClient, pgId);
            }
            throw new CommandException("One or more services may be stuck disabling, run command with -verbose to obtain more details");
        }

        // otherwise the command was successful so print the final states and return
        if (shouldPrint(properties)) {
            println();
            println("Finished States:");
            printControllerServiceStates(serviceStates);
            println();
        }

        return VoidResult.getInstance();
    }

    private void printControllerServiceStates(final ControllerServiceStateCounts serviceStates) {
        println(" - " + serviceStates.getEnabled() + " enabled");
        println(" - " + serviceStates.getEnabling() + " enabling");
        println(" - " + serviceStates.getDisabled() + " disabled");
        println(" - " + serviceStates.getDisabling() + " disabling");
    }

    private void printServicesStillDisabling(final FlowClient flowClient, final String pgId)
            throws NiFiClientException, IOException {

        final ControllerServicesEntity servicesEntity = flowClient.getControllerServices(pgId);
        if (servicesEntity == null || servicesEntity.getControllerServices() == null) {
            return;
        }

        println();
        println("One or more services appear to be stuck disabling: ");

        for (final ControllerServiceEntity serviceEntity : servicesEntity.getControllerServices()) {
            if (ControllerServiceStates.STATE_DISABLING.equals(serviceEntity.getComponent().getState())) {
                println();
                println("Service: " + serviceEntity.getId() + " - " + serviceEntity.getComponent().getName());

                if (serviceEntity.getBulletins() != null) {
                    println();
                    println("Reasons: ");
                    for (final BulletinEntity bulletinEntity : serviceEntity.getBulletins()) {
                        println("- " + bulletinEntity.getBulletin().getMessage());
                    }
                }
            }
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
    }

    private boolean shouldPrint(final Properties properties) {
        return isInteractive() || isVerbose(properties);
    }

    private ControllerServiceStateCounts getControllerServiceStates(final FlowClient flowClient, final String pgId)
            throws NiFiClientException, IOException {
        return ControllerServiceUtil.getControllerServiceStates(flowClient, pgId);
    }
}
