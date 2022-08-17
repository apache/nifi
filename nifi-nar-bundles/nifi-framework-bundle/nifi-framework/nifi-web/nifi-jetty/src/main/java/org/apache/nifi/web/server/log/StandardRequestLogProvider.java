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
package org.apache.nifi.web.server.log;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Slf4jRequestLogWriter;

/**
 * Standard implementation of Jetty Request Log Provider with configurable format string
 */
public class StandardRequestLogProvider implements RequestLogProvider {
    private static final String LOGGER_NAME = "org.apache.nifi.web.server.RequestLog";

    private final String format;

    public StandardRequestLogProvider(final String format) {
        this.format = StringUtils.defaultIfBlank(format, CustomRequestLog.EXTENDED_NCSA_FORMAT);
    }

    /**
     * Get Request Log configured using specified format and SLF4J writer
     *
     * @return Custom Request Log
     */
    @Override
    public RequestLog getRequestLog() {
        final Slf4jRequestLogWriter writer = new Slf4jRequestLogWriter();
        writer.setLoggerName(LOGGER_NAME);
        return new CustomRequestLog(writer, format);
    }
}
