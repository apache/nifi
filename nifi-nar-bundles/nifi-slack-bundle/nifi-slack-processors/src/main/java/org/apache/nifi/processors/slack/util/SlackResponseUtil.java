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
package org.apache.nifi.processors.slack.util;

import com.slack.api.methods.SlackApiException;
import com.slack.api.model.Message;
import org.apache.nifi.processors.slack.consume.PartialThreadException;
import org.apache.nifi.util.StringUtils;

import java.util.Objects;

public class SlackResponseUtil {
    public static String getErrorMessage(final String error, final String needed, final String provided, final String warning) {
        final String mainMessage = Objects.requireNonNullElse(error, warning);

        if (needed != null && provided != null) {
            return mainMessage + ": Permission needed: " + needed + "; Permission granted to this bot: " + provided;
        }

        return mainMessage;
    }

    private static Throwable getCauseOfPartialThreadException(final Throwable t) {
        if (t instanceof final PartialThreadException pte) {
            final Throwable cause = pte.getCause();
            return cause == null ? t : cause;
        }

        return t;
    }

    public static boolean isRateLimited(final Throwable throwable) {
        final Throwable cause = getCauseOfPartialThreadException(throwable);
        return cause instanceof SlackApiException && 429 == ((SlackApiException) cause).getResponse().code();
    }

    public static int getRetryAfterSeconds(final Throwable throwable) {
        final Throwable cause = getCauseOfPartialThreadException(throwable);
        if (!(cause instanceof final SlackApiException slackApiException)) {
            return 1;
        }

        return Integer.parseInt( slackApiException.getResponse().header("Retry-After", "10") );
    }

    public static boolean hasReplies(final Message message) {
        final String threadTs = message.getThreadTs();
        return !StringUtils.isEmpty(threadTs);
    }
}
