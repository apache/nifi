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
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ForkedProcesses extends AbstractFileBasedRuntimeValidator {
    private static final String FILE_PATH = String.format("/proc/%s/limits", ProcessHandle.current().pid());
    private static final Pattern PATTERN = Pattern.compile("Max processes\\s+(\\d+)\\s+(\\d+)\\s+processes\\s+");
    private static final int RECOMMENDED_SOFT_LIMIT = 10000;
    private static final int RECOMMENDED_HARD_LIMIT = 10000;

    public ForkedProcesses() {
        super(new File(FILE_PATH));
    }

    ForkedProcesses(final File configurationFile) {
        super(configurationFile);
    }

    @Override
    protected void performChecks(List<RuntimeValidatorResult> results) {
        try {
            final String forkedProcessesString = getContents();
            final Matcher matcher = PATTERN.matcher(forkedProcessesString);
            if (matcher.find()) {
                final int softLimit = Integer.valueOf(matcher.group(1));
                final int hardLimit = Integer.valueOf(matcher.group(2));
                if (softLimit < RECOMMENDED_SOFT_LIMIT) {
                    final RuntimeValidatorResult result =  new RuntimeValidatorResult.Builder()
                            .subject(this.getClass().getName())
                            .outcome(RuntimeValidatorResult.Outcome.FAILED)
                            .explanation(String.format("Soft limit for forked processes [%d] is less than recommended soft limit [%d]", softLimit, RECOMMENDED_SOFT_LIMIT))
                            .build();
                    results.add(result);
                }
                if (hardLimit < RECOMMENDED_HARD_LIMIT) {
                    final RuntimeValidatorResult result =  new RuntimeValidatorResult.Builder()
                            .subject(this.getClass().getName())
                            .outcome(RuntimeValidatorResult.Outcome.FAILED)
                            .explanation(String.format("Hard limit for forked processes [%d] is less than recommended hard limit [%d]", hardLimit, RECOMMENDED_HARD_LIMIT))
                            .build();
                    results.add(result);
                }
            } else {
                final RuntimeValidatorResult result = new RuntimeValidatorResult.Builder()
                        .subject(this.getClass().getName())
                        .outcome(RuntimeValidatorResult.Outcome.FAILED)
                        .explanation(String.format("Configuration file [%s] cannot be parsed", getConfigurationFile().getAbsolutePath()))
                        .build();
                results.add(result);
            }
        } catch (final IOException e) {
            final RuntimeValidatorResult result = new RuntimeValidatorResult.Builder()
                    .subject(this.getClass().getName())
                    .outcome(RuntimeValidatorResult.Outcome.FAILED)
                    .explanation(String.format("Configuration file [%s] cannot be read", getConfigurationFile().getAbsolutePath()))
                    .build();
            results.add(result);
        }
    }
}
