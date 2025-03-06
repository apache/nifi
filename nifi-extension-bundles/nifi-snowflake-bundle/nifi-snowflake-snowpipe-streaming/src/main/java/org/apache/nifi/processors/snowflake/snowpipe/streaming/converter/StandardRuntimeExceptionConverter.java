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
package org.apache.nifi.processors.snowflake.snowpipe.streaming.converter;

import net.snowflake.ingest.streaming.internal.StreamingIngestResponseCode;

import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Standard Runtime Exception Converter supporting resolution of specific Snowpipe Streaming Channel status codes
 */
public class StandardRuntimeExceptionConverter implements RuntimeExceptionConverter {
    private static final Pattern STATUS_CODE_PATTERN = Pattern.compile("\"status_code\".*?(\\d+)", Pattern.CASE_INSENSITIVE);

    @Override
    public RuntimeException convertException(final Throwable exception) {
        Objects.requireNonNull(exception, "Exception required");
        final ChannelErrorStatus status = getErrorStatus(exception.getMessage())
                .orElse(ChannelErrorStatus.UNKNOWN_ERROR);
        return new ChannelErrorStatusException(status, exception);
    }

    private Optional<ChannelErrorStatus> getErrorStatus(final String message) {
        final Matcher matcher = STATUS_CODE_PATTERN.matcher(message);
        if (matcher.find()) {
            final long statusCodeValue = Long.parseLong(matcher.group(1));
            if (statusCodeValue == StreamingIngestResponseCode.ERR_TABLE_DOES_NOT_EXIST_NOT_AUTHORIZED.getStatusCode()) {
                return Optional.of(ChannelErrorStatus.TABLE_NOT_FOUND);
            } else if (statusCodeValue == StreamingIngestResponseCode.ERR_DATABASE_DOES_NOT_EXIST_OR_NOT_AUTHORIZED.getStatusCode()) {
                return Optional.of(ChannelErrorStatus.DATABASE_NOT_FOUND);
            } else if (statusCodeValue == StreamingIngestResponseCode.ERR_SCHEMA_DOES_NOT_EXIST_OR_NOT_AUTHORIZED.getStatusCode()) {
                return Optional.of(ChannelErrorStatus.SCHEMA_NOT_FOUND);
            }
        }
        return Optional.empty();
    }
}
