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

package org.apache.nifi.toolkit.cli.impl.command.nifi.params;

import org.apache.commons.cli.MissingOptionException;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.VoidResult;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

public class GetAsset extends AbstractNiFiCommand<VoidResult> {

    public GetAsset() {
        super("get-asset", VoidResult.class);
    }

    @Override
    public String getDescription() {
        return "Retrieves the content of the given asset.";
    }

    @Override
    protected void doInitialize(Context context) {
        super.doInitialize(context);
        addOption(CommandOption.PARAM_CONTEXT_ID.createOption());
        addOption(CommandOption.ASSET_ID.createOption());
        addOption(CommandOption.OUTPUT_DIR.createOption());
    }

    @Override
    public VoidResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {
        final String paramContextId = getRequiredArg(properties, CommandOption.PARAM_CONTEXT_ID);
        final String assetId = getRequiredArg(properties, CommandOption.ASSET_ID);
        final File outputDir = new File(getRequiredArg(properties, CommandOption.OUTPUT_DIR));
        final Path assetFile = client.getParamContextClient().getAssetContent(paramContextId, assetId, outputDir);
        if (isInteractive()) {
            println(assetFile.toFile().getAbsolutePath());
        }
        return VoidResult.getInstance();
    }
}
