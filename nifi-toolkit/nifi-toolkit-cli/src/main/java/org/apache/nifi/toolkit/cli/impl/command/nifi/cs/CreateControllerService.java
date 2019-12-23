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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ControllerClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.StringResult;
import org.apache.nifi.toolkit.cli.impl.util.JacksonUtils;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Command for creating a controller service for reporting tasks.
 */
public class CreateControllerService extends AbstractNiFiCommand<StringResult> {

    public CreateControllerService() {
        super("create-service", StringResult.class);
    }

    @Override
    public String getDescription() {
        return "Creates a controller service for reporting tasks from a local file.";
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.INPUT_SOURCE.createOption());
    }

    @Override
    public StringResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {
        final String inputFile = getRequiredArg(properties, CommandOption.INPUT_SOURCE);
        final URI uri = Paths.get(inputFile).toAbsolutePath().toUri();
        final String contents = IOUtils.toString(uri, StandardCharsets.UTF_8);

        final ObjectMapper objectMapper = JacksonUtils.getObjectMapper();
        final ControllerServiceEntity deserializedService = objectMapper.readValue(contents, ControllerServiceEntity.class);
        if (deserializedService == null) {
            throw new IOException("Unable to deserialize controller service version from " + inputFile);
        }

        deserializedService.setRevision(getInitialRevisionDTO());

        final ControllerClient controllerClient = client.getControllerClient();
        final ControllerServiceEntity createdEntity = controllerClient.createControllerService(deserializedService);

        return new StringResult(String.valueOf(createdEntity.getId()), getContext().isInteractive());
    }

}
