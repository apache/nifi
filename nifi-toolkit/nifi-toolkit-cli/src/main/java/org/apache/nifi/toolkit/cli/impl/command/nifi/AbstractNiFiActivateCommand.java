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
package org.apache.nifi.toolkit.cli.impl.command.nifi;

import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.result.VoidResult;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ComponentEntity;
import org.apache.nifi.web.api.entity.ComponentRunStatusEntity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Base class for NiFi compornent activation commands.
 */
public abstract class AbstractNiFiActivateCommand<C extends ComponentEntity, S extends ComponentRunStatusEntity>
        extends AbstractNiFiCommand<VoidResult> {

    public AbstractNiFiActivateCommand(final String name) {
        super(name, VoidResult.class);
    }

    protected void activate(final NiFiClient client, final Properties properties,
            final Set<C> componentEntities, final String state) throws IOException, CommandException {
        if (shouldPrint(properties)) {
            println();
        }

        final List<NiFiClientException> exceptions = new ArrayList<>();

        for (final C componentEntity : componentEntities) {
            final RevisionDTO revisionDTO = new RevisionDTO();
            revisionDTO.setVersion(componentEntity.getRevision().getVersion());
            revisionDTO.setClientId(getContext().getSession().getNiFiClientID());

            final S runStatusEntity = getRunStatusEntity();
            runStatusEntity.setRevision(revisionDTO);
            runStatusEntity.setState(state);
            runStatusEntity.validateState();

            try {
                final C activated = activateComponent(client, componentEntity, runStatusEntity);

                if (shouldPrint(properties)) {
                    println(getDispName(activated) + " has been " + state.toLowerCase());
                    println();
                }
            } catch (NiFiClientException e) {
                exceptions.add(e);

                if (shouldPrint(properties)) {
                    println(getDispName(componentEntity) + " could not be " + state.toLowerCase());
                    println();
                    e.printStackTrace();
                }
            }
        }

        if (exceptions.size() > 0) {
            throw new CommandException(exceptions.size() + " components could not be " + state.toLowerCase() + ", " +
                    "run command with -verbose to obtain more details");
        }
    }

    private boolean shouldPrint(final Properties properties) {
        return isInteractive() || isVerbose(properties);
    }

    public abstract S getRunStatusEntity();

    public abstract C activateComponent(final NiFiClient client, final C componentEntity, final S runStatusEntity)
            throws NiFiClientException, IOException;

    public abstract String getDispName(final C componentEntity);
}
