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
package org.apache.nifi.toolkit.cli.impl.command.nifi.templates;

import org.apache.commons.cli.MissingOptionException;
import org.apache.nifi.persistence.TemplateDeserializer;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ProcessGroupClient;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.StringResult;
import org.apache.nifi.web.api.entity.TemplateEntity;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Command for uploading a template file.
 */
public class UploadTemplate extends AbstractNiFiCommand<StringResult> {

    public UploadTemplate() {
        super("upload-template", StringResult.class);
    }

    @Override
    public String getDescription() {
        return "Uploads a local template file.";
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.PG_ID.createOption());
        addOption(CommandOption.INPUT_SOURCE.createOption());
    }

    @Override
    public StringResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {
        final String processGroupId = getRequiredArg(properties, CommandOption.PG_ID);
        final String inputFile = getRequiredArg(properties, CommandOption.INPUT_SOURCE);

        final FileInputStream file = new FileInputStream(Paths.get(inputFile).toAbsolutePath().toFile());

        final ProcessGroupClient pgClient = client.getProcessGroupClient();
        final TemplateEntity createdEntity = pgClient.uploadTemplate(
                processGroupId,
                TemplateDeserializer.deserialize(file));

        return new StringResult(String.valueOf(createdEntity.getTemplate().getId()), getContext().isInteractive());
    }

}
