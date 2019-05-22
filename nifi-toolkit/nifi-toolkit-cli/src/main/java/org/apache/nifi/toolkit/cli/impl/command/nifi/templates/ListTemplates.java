/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.toolkit.cli.impl.command.nifi.templates;

import org.apache.commons.cli.MissingOptionException;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.nifi.TemplatesResult;
import org.apache.nifi.web.api.entity.TemplatesEntity;

import java.io.IOException;
import java.util.Properties;

/**
 * Command to get the list of templates.
 */
public class ListTemplates extends AbstractNiFiCommand<TemplatesResult> {

    public ListTemplates() {
        super("list-templates", TemplatesResult.class);
    }

    @Override
    public String getDescription() {
        return "Retrieves the list of templates";
    }

    @Override
    public TemplatesResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {
        final TemplatesEntity templatesEntity = client.getFlowClient().getTemplates();
        return new TemplatesResult(getResultType(properties), templatesEntity);
    }
}
