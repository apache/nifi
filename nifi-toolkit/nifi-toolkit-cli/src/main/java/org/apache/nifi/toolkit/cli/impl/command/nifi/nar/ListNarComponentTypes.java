/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.toolkit.cli.impl.command.nifi.nar;

import org.apache.commons.cli.MissingOptionException;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.nifi.NarComponentTypesResult;
import org.apache.nifi.toolkit.client.ControllerClient;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.NarDetailsEntity;

import java.io.IOException;
import java.util.Properties;

public class ListNarComponentTypes extends AbstractNiFiCommand<NarComponentTypesResult> {

    public ListNarComponentTypes() {
        super("list-nar-component-types", NarComponentTypesResult.class);
    }

    @Override
    public String getDescription() {
        return "Lists the available component types for a given NAR in the NAR Manager";
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.NAR_ID.createOption());
    }

    @Override
    public NarComponentTypesResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {
        final String narId = getRequiredArg(properties, CommandOption.NAR_ID);
        final ControllerClient controllerClient = client.getControllerClient();
        final NarDetailsEntity detailsEntity = controllerClient.getNarDetails(narId);
        return new NarComponentTypesResult(getResultType(properties), detailsEntity);
    }
}
