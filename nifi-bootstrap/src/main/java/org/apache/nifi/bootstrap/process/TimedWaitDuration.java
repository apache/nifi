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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TimedWaitDuration implements RuntimeValidator {
    private static final String[] POSSIBLE_DIRECTORIES = new String[] {
            "/proc/sys/net/ipv4/tcp_tw_timeout",
            "/proc/sys/net/netfilter/nf_conntrack_tcp_timeout_time_wait",
            "/proc/sys/net/ipv4/netfilter/ip_conntrack_tcp_timeout_time_wait"
    };
    private static final String DIGITS_REGEX = "\\d+";
    private static final Pattern PATTERN = Pattern.compile(DIGITS_REGEX);
    private static final int DESIRED_TIMED_WAIT_DURATION = 1;

    private File configurationFile;

    public TimedWaitDuration() {
    }

    TimedWaitDuration(final File configurationFile) {
        this.configurationFile = configurationFile;
    }

    @Override
    public List<RuntimeValidatorResult> validate() {
        final List<RuntimeValidatorResult> results = new ArrayList<>();
        if (configurationFile == null) {
            determineConfigurationFile();
            if (configurationFile == null) {
                final RuntimeValidatorResult result = new RuntimeValidatorResult.Builder()
                        .subject(this.getClass().getName())
                        .outcome(RuntimeValidatorResult.Outcome.SKIPPED)
                        .explanation("Configuration file for timed wait duration cannot be determined")
                        .build();
                results.add(result);
                return results;
            }
        }
        if (!configurationFile.canRead()) {
            final RuntimeValidatorResult result = new RuntimeValidatorResult.Builder()
                    .subject(this.getClass().getName())
                    .outcome(RuntimeValidatorResult.Outcome.SKIPPED)
                    .explanation(String.format("Configuration file [%s] cannot be read", configurationFile.getAbsolutePath()))
                    .build();
            results.add(result);
            return results;
        }

        try {
            final Scanner scanner = new Scanner(configurationFile);
            final StringBuilder sb = new StringBuilder();
            while (scanner.hasNextLine()) {
                sb.append(scanner.nextLine());
            }
            scanner.close();
            final String timedWaitDurationString = sb.toString();
            final Matcher matcher = PATTERN.matcher(timedWaitDurationString);
            if (matcher.find()) {
                final int timedWaitDuration = Integer.valueOf(matcher.group());
                if (timedWaitDuration > DESIRED_TIMED_WAIT_DURATION) {
                    final RuntimeValidatorResult result =  new RuntimeValidatorResult.Builder()
                            .subject(this.getClass().getName())
                            .outcome(RuntimeValidatorResult.Outcome.FAILED)
                            .explanation(
                                    String.format(
                                            "Timed wait duration [%ds] is more than the advised timed wait duration [%ds]",
                                            timedWaitDuration,
                                            DESIRED_TIMED_WAIT_DURATION
                                    )
                            )
                            .build();
                    results.add(result);
                }
            } else {
                final RuntimeValidatorResult result = new RuntimeValidatorResult.Builder()
                        .subject(this.getClass().getName())
                        .outcome(RuntimeValidatorResult.Outcome.FAILED)
                        .explanation(String.format("Configuration file [%s] cannot be parsed", configurationFile.getAbsolutePath()))
                        .build();
                results.add(result);
            }
        } catch (final IOException e) {
            final RuntimeValidatorResult result = new RuntimeValidatorResult.Builder()
                    .subject(this.getClass().getName())
                    .outcome(RuntimeValidatorResult.Outcome.FAILED)
                    .explanation(String.format("Configuration file [%s] cannot be read", configurationFile.getAbsolutePath()))
                    .build();
            results.add(result);
        }

        if (results.isEmpty()) {
            final RuntimeValidatorResult result = new RuntimeValidatorResult.Builder()
                    .subject(this.getClass().getName())
                    .outcome(RuntimeValidatorResult.Outcome.SUCCESSFUL)
                    .build();
            results.add(result);
        }
        return results;
    }

    private void determineConfigurationFile() {
        for (final String directory: POSSIBLE_DIRECTORIES) {
            final File file = new File(directory);
            if (file.canRead()) {
                configurationFile = file;
                return;
            }
        }
    }
}
