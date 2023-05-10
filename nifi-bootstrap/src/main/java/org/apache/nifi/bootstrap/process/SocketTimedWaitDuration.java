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
package org.apache.nifi.bootstrap.process;

import java.io.File;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SocketTimedWaitDuration extends AbstractFileBasedRuntimeValidator {
    // Different Linux Kernel versions have different file paths for TIMED_WAIT_DURATION
    private static final String[] POSSIBLE_FILE_PATHS = new String[] {
            "/proc/sys/net/ipv4/tcp_tw_timeout",
            "/proc/sys/net/netfilter/nf_conntrack_tcp_timeout_time_wait",
            "/proc/sys/net/ipv4/netfilter/ip_conntrack_tcp_timeout_time_wait"
    };
    private static final Pattern PATTERN = Pattern.compile("\\d+");
    private static final int DESIRED_TIMED_WAIT_DURATION = 1;

    public SocketTimedWaitDuration() {
        super(determineConfigurationFile());
    }

    SocketTimedWaitDuration(final File configurationFile) {
        super(configurationFile);
    }

    @Override
    protected Pattern getPattern() {
        return PATTERN;
    }

    @Override
    protected void performChecks(final Matcher matcher, final List<RuntimeValidatorResult> results) {
        final String configurationPath = getConfigurationFile().getAbsolutePath();
        if (matcher.find()) {
            final int timedWaitDuration = Integer.parseInt(matcher.group());
            if (timedWaitDuration > DESIRED_TIMED_WAIT_DURATION) {
                final RuntimeValidatorResult result =  getResultBuilder(RuntimeValidatorResult.Outcome.FAILED)
                        .explanation(
                                String.format(
                                        "TCP Socket Wait [%d seconds] more than recommended [%d seconds] according to [%s]",
                                        timedWaitDuration,
                                        DESIRED_TIMED_WAIT_DURATION,
                                        configurationPath
                                )
                        )
                        .build();
                results.add(result);
            }
        } else {
            final RuntimeValidatorResult result = getResultBuilder(RuntimeValidatorResult.Outcome.FAILED)
                    .explanation(String.format("Configuration file [%s] cannot be parsed", getConfigurationFile().getAbsolutePath()))
                    .build();
            results.add(result);
        }
    }

    private static File determineConfigurationFile() {
        for (final String filePath: POSSIBLE_FILE_PATHS) {
            final File file = new File(filePath);
            if (file.canRead()) {
                return file;
            }
        }
        return null;
    }
}
