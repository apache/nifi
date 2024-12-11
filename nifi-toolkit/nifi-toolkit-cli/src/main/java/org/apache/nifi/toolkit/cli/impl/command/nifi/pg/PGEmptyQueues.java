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
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.VoidResult;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.toolkit.client.ProcessGroupClient;
import org.apache.nifi.web.api.entity.DropRequestEntity;

import java.io.IOException;
import java.util.Properties;

/**
 * Command to empty all queues of a process group.
 */
public class PGEmptyQueues extends AbstractNiFiCommand<VoidResult> {

    public static final int MAX_ITERATIONS = 20;
    public static final long DELAY_MS = 1000;

    public PGEmptyQueues() {
        super("pg-empty-queues", VoidResult.class);
    }

    @Override
    public String getDescription() {
        return "Empty all queues, recursively, in the specified Process Group. It is recommended to first use pg-stop.";
    }

    @Override
    protected void doInitialize(final Context context) {
        addOption(CommandOption.PG_ID.createOption());
    }

    @Override
    public VoidResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException {

        final String pgId = getRequiredArg(properties, CommandOption.PG_ID);

        final ProcessGroupClient pgClient = client.getProcessGroupClient();
        DropRequestEntity requestEntity = pgClient.emptyQueues(pgId);
        final String requestId = requestEntity.getDropRequest().getId();

        int iterations = 1;
        while (!requestEntity.getDropRequest().isFinished() && iterations < MAX_ITERATIONS) {
            if (shouldPrint(properties)) {
                println("Emptying queues, currently at " + requestEntity.getDropRequest().getPercentCompleted() + "% ("
                        + iterations + " of " + MAX_ITERATIONS + ")...");
            }
            sleep(DELAY_MS);
            iterations++;
            requestEntity = pgClient.getEmptyQueuesRequest(pgId, requestId);
        }

        if (shouldPrint(properties)) {
            if (requestEntity.getDropRequest().isFinished()) {
                println("Drop request completed. Deleted: " + requestEntity.getDropRequest().getDropped());
            } else {
                println("Drop request didn't complete yet. Thus far, deleted: " + requestEntity.getDropRequest().getDropped());
            }
        }

        return VoidResult.getInstance();
    }

    private boolean shouldPrint(final Properties properties) {
        return isInteractive() || isVerbose(properties);
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
    }

}
