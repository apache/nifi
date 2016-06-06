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
package org.apache.nifi.remote.util;

import org.apache.nifi.events.EventReporter;
import org.apache.nifi.reporting.Severity;
import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

public class EventReportUtil {

    private static final String CATEGORY = "Site-to-Site";

    public static void warn(final Logger logger, final EventReporter eventReporter, final String msg, final Object... args) {
        logger.warn(msg, args);
        if (eventReporter != null) {
            eventReporter.reportEvent(Severity.WARNING, CATEGORY, MessageFormatter.arrayFormat(msg, args).getMessage());
        }
    }

    public static void warn(final Logger logger, final EventReporter eventReporter, final String msg, final Throwable t) {
        logger.warn(msg, t);

        if (eventReporter != null) {
            eventReporter.reportEvent(Severity.WARNING, CATEGORY, msg + ": " + t.toString());
        }
    }

    public static void error(final Logger logger, final EventReporter eventReporter, final String msg, final Object... args) {
        logger.error(msg, args);
        if (eventReporter != null) {
            eventReporter.reportEvent(Severity.ERROR, CATEGORY, MessageFormatter.arrayFormat(msg, args).getMessage());
        }
    }

}
