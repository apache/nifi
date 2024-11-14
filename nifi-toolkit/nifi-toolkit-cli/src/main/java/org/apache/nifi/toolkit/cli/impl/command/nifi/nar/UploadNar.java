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
import org.apache.nifi.toolkit.cli.impl.result.nifi.NarUploadResult;
import org.apache.nifi.toolkit.client.ControllerClient;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.NarSummaryDTO;
import org.apache.nifi.web.api.entity.NarSummaryEntity;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class UploadNar extends AbstractNiFiCommand<NarUploadResult> {

    private static final int MAX_TIMEOUT_SECONDS = 600;
    private static final int DEFAULT_TIMEOUT_SECONDS = 60;
    private static final long POLL_INTERVAL_MILLIS = 2000;

    public UploadNar() {
        super("upload-nar", NarUploadResult.class);
    }

    @Override
    public String getDescription() {
        return "Uploads a NAR to the NAR Manager";
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.NAR_FILE.createOption());
        addOption(CommandOption.NAR_UPLOAD_TIMEOUT.createOption());
    }

    @Override
    public NarUploadResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {
        final ControllerClient controllerClient = client.getControllerClient();
        final File narFile = new File(getRequiredArg(properties, CommandOption.NAR_FILE));
        final NarSummaryEntity summaryEntity = uploadNar(controllerClient, narFile);
        final String narId = summaryEntity.getNarSummary().getIdentifier();

        final int timeoutSeconds = getProcessingTimeout(properties);
        final int pollIterations = Math.max(Long.valueOf(timeoutSeconds * 1000L / POLL_INTERVAL_MILLIS).intValue(), 1);
        final int maxPollIterations = Long.valueOf(MAX_TIMEOUT_SECONDS * 1000L / POLL_INTERVAL_MILLIS).intValue();
        final int selectedPollIterations = Math.min(pollIterations, maxPollIterations);

        NarSummaryEntity progressEntity = null;
        for (int i = 0; i < selectedPollIterations; i++) {
            progressEntity = controllerClient.getNarSummary(narId);
            final NarSummaryDTO progressDto = progressEntity.getNarSummary();
            if (progressDto.isInstallComplete()) {
                return new NarUploadResult(getResultType(properties), progressEntity);
            }

            try {
                if (getContext().isInteractive()) {
                    println("Waiting for NAR install to complete, current state is: " + progressDto.getState());
                }
                Thread.sleep(POLL_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        println("NAR install has not completed within the given timeout, the install will continue to run and may complete later");
        return new NarUploadResult(getResultType(properties), progressEntity);
    }

    private NarSummaryEntity uploadNar(final ControllerClient controllerClient, final File narFile) throws NiFiClientException, IOException {
        try (final InputStream inputStream = new FileInputStream(narFile)) {
            return controllerClient.uploadNar(narFile.getName(), inputStream);
        }
    }

    protected int getProcessingTimeout(final Properties properties) {
        try {
            final Integer updateTimeoutSeconds = getIntArg(properties, CommandOption.UPDATE_TIMEOUT);
            return updateTimeoutSeconds == null ? DEFAULT_TIMEOUT_SECONDS : updateTimeoutSeconds;
        } catch (final MissingOptionException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

}
