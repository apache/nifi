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
import org.apache.nifi.toolkit.cli.impl.result.VoidResult;
import org.apache.nifi.web.api.entity.ScheduleComponentsEntity;

import java.io.IOException;
import java.util.Properties;

/**
 * Command to start the components of a process group.
 */
public class PGStart extends AbstractNiFiCommand<VoidResult> {

    public PGStart() {
        super("pg-start", VoidResult.class);
    }

    @Override
    public String getDescription() {
        return "Starts the given process group which starts any enabled and valid components contained in that group.";
    }

    @Override
    protected void doInitialize(final Context context) {
        addOption(CommandOption.PG_ID.createOption());
    }

    @Override
    public VoidResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {

        final String pgId = getRequiredArg(properties, CommandOption.PG_ID);

        final ScheduleComponentsEntity entity = new ScheduleComponentsEntity();
        entity.setId(pgId);
        entity.setState(ScheduleComponentsEntity.STATE_RUNNING);

        final FlowClient flowClient = client.getFlowClient();
        final ScheduleComponentsEntity resultEntity = flowClient.scheduleProcessGroupComponents(pgId, entity);
        return VoidResult.getInstance();
    }

}
