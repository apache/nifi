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

public abstract class AbstractFileBasedRuntimeValidator implements RuntimeValidator {
    private final File configurationFile;

    AbstractFileBasedRuntimeValidator(final File configurationFile) {
        this.configurationFile = configurationFile;
    }

    @Override
    public List<RuntimeValidatorResult> validate() {
        final List<RuntimeValidatorResult> results = new ArrayList<>();
        if (!canReadConfigurationFile(results)) {
            return results;
        }

        try {
            final Pattern pattern = getPattern();
            final Matcher matcher = pattern.matcher(getContents());
            performChecks(matcher, results);
        } catch (final IOException e) {
            final RuntimeValidatorResult result = getResultBuilder(RuntimeValidatorResult.Outcome.FAILED)
                    .explanation(String.format("Configuration file [%s] cannot be read", getConfigurationFile().getAbsolutePath()))
                    .build();
            results.add(result);
        }

        processResults(results);
        return results;
    }

    protected RuntimeValidatorResult.Builder getResultBuilder(final RuntimeValidatorResult.Outcome outcome) {
        return new RuntimeValidatorResult.Builder().subject(getClass().getSimpleName()).outcome(outcome);
    }

    protected File getConfigurationFile() {
        return configurationFile;
    }

    protected String getContents() throws IOException {
        // using Scanner to read file because reading whole lines for virtual files
        // in Linux /proc/sys directory fail when using other implementations
        try (Scanner scanner = new Scanner(configurationFile)) {
            final StringBuilder builder = new StringBuilder();
            while (scanner.hasNextLine()) {
                builder.append(scanner.nextLine());
                builder.append(System.lineSeparator());
            }
            return builder.toString();
        }
    }

    protected abstract Pattern getPattern();

    protected abstract void performChecks(final Matcher matcher, final List<RuntimeValidatorResult> results);

    private boolean canReadConfigurationFile(final List<RuntimeValidatorResult> results) {
        final File configurationFile = getConfigurationFile();
        if (configurationFile == null) {
            final RuntimeValidatorResult result = getResultBuilder(RuntimeValidatorResult.Outcome.SKIPPED)
                    .explanation("Configuration file not found")
                    .build();
            results.add(result);
            return false;
        }
        if (!configurationFile.canRead()) {
            final RuntimeValidatorResult result = getResultBuilder(RuntimeValidatorResult.Outcome.SKIPPED)
                    .explanation(String.format("Configuration file [%s] cannot be read", configurationFile.getAbsolutePath()))
                    .build();
            results.add(result);
            return false;
        }
        return true;
    }

    private void processResults(final List<RuntimeValidatorResult> results) {
        if (results.isEmpty()) {
            final RuntimeValidatorResult result = getResultBuilder(RuntimeValidatorResult.Outcome.SUCCESSFUL).build();
            results.add(result);
        }
    }
}
