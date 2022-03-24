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
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.entity.ActivateControllerServicesEntity;
import org.apache.nifi.web.api.entity.BulletinEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServicesEntity;

import java.io.IOException;
import java.util.Properties;

public class PGEnableControllerServices extends AbstractNiFiCommand<VoidResult> {

    public static final int MAX_ATTEMPTS = 180;
    public static final int MAX_ENABLING_ITERATIONS = 20;
    public static final long ENABLING_DELAY_MS = 2000;

    public PGEnableControllerServices() {
        super("pg-enable-services", VoidResult.class);
    }

    @Override
    public String getDescription() {
        return "Attempts to enable all controller services in the given PG. In stand-alone mode this command will not " +
                "produce all of the output seen in interactive mode unless the --verbose argument is specified.";
    }

    @Override
    protected void doInitialize(final Context context) {
        addOption(CommandOption.PG_ID.createOption());
    }

    @Override
    public VoidResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {

        final String pgId = getRequiredArg(properties, CommandOption.PG_ID);
        final FlowClient flowClient = client.getFlowClient();

        if (shouldPrint(properties)) {
            println();
        }

        int count = 0;
        int prevNumEnabled = -1;
        int enablingIterations = 1;

        // request to enable services until the number of enabled services is no longer changing, which means either all
        // services have been enabled, or the rest of the services are invalid and can't be enabled
        while (count < MAX_ATTEMPTS) {

            // retrieve the current states of the services in the given pg
            final ControllerServiceStateCounts states = getControllerServiceStates(flowClient, pgId);

            // if any services are currently enabling then sleep and loop again
            if (states.getEnabling() > 0) {
                if (enablingIterations < MAX_ENABLING_ITERATIONS) {
                    if (shouldPrint(properties)) {
                        println("Currently " + states.getEnabling() + " services are enabling, waiting to finish before " +
                                "proceeding (" + enablingIterations + " of " + MAX_ENABLING_ITERATIONS + ")");
                    }
                    try {
                        Thread.sleep(ENABLING_DELAY_MS);
                    } catch (InterruptedException e) {
                        Thread.interrupted();
                    }

                    enablingIterations++;
                    continue;
                } else {
                    if (shouldPrint(properties)) {
                        printServicesStillEnabling(flowClient, pgId);
                    }

                    // throw an exception so stand-alone mode will exit with a non-zero status code
                    throw new CommandException("One or more services are stuck enabling, run command with -verbose to obtain more details");
                }
            }

            // reset the enabling iteration count since we got past the above block without breaking
            enablingIterations = 1;

            // if no services are enabling and the number of enabled services equals the number of enabled services from
            // last iteration, then we know there are no more that we can enable so break
            if (states.getEnabled() == prevNumEnabled && states.getEnabling() == 0) {
                if (shouldPrint(properties)) {
                    println();
                    println("Finished with " + states.getEnabled() + " enabled services and " + states.getDisabled() + " disabled services");
                }

                if (states.getDisabled() > 0 || states.getDisabling() > 0) {
                    if (shouldPrint(properties)) {
                        printServicesNotEnabled(flowClient, pgId);
                        println();
                    }

                    // throw an exception so stand-alone mode will exit with a non-zero status code
                    throw new CommandException("One or more services could not be enabled, run command with -verbose to obtain more details");
                } else {
                    if (shouldPrint(properties)) {
                        println();
                    }

                    // just break here to proceed with normal completion (i.e. will have a zero status code)
                    break;
                }
            }

            // if we didn't break then store the number that were enabled to compare with next time
            prevNumEnabled = states.getEnabled();

            if (shouldPrint(properties)) {
                println("Currently " + states.getEnabled() + " enabled services and " + states.getDisabled()
                        + " disabled services, attempting to enable services...");
            }

            // send the request to enable services
            final ActivateControllerServicesEntity enableEntity = new ActivateControllerServicesEntity();
            enableEntity.setId(pgId);
            enableEntity.setState(ActivateControllerServicesEntity.STATE_ENABLED);

            flowClient.activateControllerServices(enableEntity);
            count++;
        }

        return VoidResult.getInstance();
    }

    private boolean shouldPrint(final Properties properties) {
        return isInteractive() || isVerbose(properties);
    }

    private ControllerServiceStateCounts getControllerServiceStates(final FlowClient flowClient, final String pgId)
            throws NiFiClientException, IOException {
        return ControllerServiceUtil.getControllerServiceStates(flowClient, pgId);
    }

    private void printServicesStillEnabling(final FlowClient flowClient, final String pgId)
            throws NiFiClientException, IOException {

        final ControllerServicesEntity servicesEntity = flowClient.getControllerServices(pgId);
        if (servicesEntity == null || servicesEntity.getControllerServices() == null) {
            return;
        }

        println();
        println("One or more services appear to be stuck enabling: ");

        for (final ControllerServiceEntity serviceEntity : servicesEntity.getControllerServices()) {
            if (ControllerServiceStates.STATE_ENABLING.equals(serviceEntity.getComponent().getState())) {
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

    private void printServicesNotEnabled(final FlowClient flowClient, final String pgId)
            throws NiFiClientException, IOException {

        final ControllerServicesEntity servicesEntity = flowClient.getControllerServices(pgId);
        if (servicesEntity == null || servicesEntity.getControllerServices() == null) {
            return;
        }

        println();
        println("The following services could not be enabled: ");

        for (final ControllerServiceEntity serviceEntity : servicesEntity.getControllerServices()) {
            if (!ControllerServiceStates.STATE_ENABLED.equals(serviceEntity.getComponent().getState())) {
                println();
                println("Service: " + serviceEntity.getId() + " - " + serviceEntity.getComponent().getName());

                final ControllerServiceDTO serviceDTO = serviceEntity.getComponent();
                if (serviceDTO.getValidationErrors() != null) {
                    println();
                    println("Validation Errors: ");
                    for (final String validationError : serviceDTO.getValidationErrors()) {
                        println("- " + validationError);
                    }
                }
            }
        }
    }

}
