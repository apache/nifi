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
package org.apache.nifi.toolkit.cli.impl.command.nifi.params;

import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.api.Result;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ParamContextClient;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextUpdateRequestEntity;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractUpdateParamContextCommand<R extends Result> extends AbstractNiFiCommand<R> {

    private static final int MAX_TIMEOUT_SECONDS = 600;
    private static final int DEFAULT_TIMEOUT_SECONDS = 60;
    private static final long POLL_INTERVAL_MILLIS = 2000;

    public AbstractUpdateParamContextCommand(final String name, final Class<R> resultClass) {
        super(name, resultClass);
    }

    protected ParameterContextUpdateRequestEntity performUpdate(final ParamContextClient client, final ParameterContextEntity parameterContextEntity,
                                                                final ParameterContextUpdateRequestEntity updateRequestEntity, final int updateTimeoutSeconds)
            throws NiFiClientException, IOException {

        final int maxPollIterations = Math.min(Math.max(Long.valueOf(updateTimeoutSeconds * 1000L / POLL_INTERVAL_MILLIS).intValue(), 1), MAX_TIMEOUT_SECONDS);

        final AtomicBoolean cancelled = new AtomicBoolean(false);

        // poll the update request for up to 30 seconds to see if it has completed
        // if it doesn't complete then an exception will be thrown, but in either case the request will be deleted
        final String contextId = parameterContextEntity.getId();
        final String updateRequestId = updateRequestEntity.getRequest().getRequestId();
        try {
            boolean completed = false;
            for (int i = 0; i < maxPollIterations; i++) {
                final ParameterContextUpdateRequestEntity retrievedUpdateRequest = client.getParamContextUpdateRequest(contextId, updateRequestId);
                if (retrievedUpdateRequest != null && retrievedUpdateRequest.getRequest().isComplete()) {
                    completed = true;
                    break;
                } else {
                    try {
                        if (getContext().isInteractive()) {
                            println("Waiting for update request to complete...");
                        }
                        Thread.sleep(POLL_INTERVAL_MILLIS);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            if (!completed) {
                cancelled.set(true);
            }

        } finally {
            final ParameterContextUpdateRequestEntity deleteUpdateRequest = client.deleteParamContextUpdateRequest(contextId, updateRequestId);

            final String failureReason = deleteUpdateRequest.getRequest().getFailureReason();
            if (!StringUtils.isBlank(failureReason)) {
                throw new NiFiClientException(failureReason);
            }

            if (cancelled.get()) {
                throw new NiFiClientException("Unable to update parameter context in time, cancelling update request");
            }

            return deleteUpdateRequest;
        }
    }

    protected int getUpdateTimeout(final Properties properties) {
        try {
            final Integer updateTimeoutSeconds = getIntArg(properties, CommandOption.UPDATE_TIMEOUT);
            return updateTimeoutSeconds == null ? DEFAULT_TIMEOUT_SECONDS : updateTimeoutSeconds;
        } catch (final MissingOptionException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
