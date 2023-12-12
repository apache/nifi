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
package org.apache.nifi.questdb.embedded;

import org.apache.nifi.questdb.DatabaseManager;
import org.apache.nifi.questdb.rollover.RolloverStrategy;

import java.io.File;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class EmbeddedDatabaseManagerBuilder {
    private SimpleEmbeddedDatabaseManagerContext context;

    private EmbeddedDatabaseManagerBuilder(final String persistPath) {
        this.context = new SimpleEmbeddedDatabaseManagerContext();
        context.setPersistLocation(persistPath);
    }

    public EmbeddedDatabaseManagerBuilder lockAttemptTime(final int lockAttemptTime, final TimeUnit lockAttemptTimeUnit) {
        context.setLockAttemptTime(lockAttemptTime);
        context.setLockAttemptTimeUnit(lockAttemptTimeUnit);
        return this;
    }

    public EmbeddedDatabaseManagerBuilder rolloverFrequency(final int rolloverFrequency, final TimeUnit rolloverFrequencyTimeUnit) {
        context.setRolloverFrequency(rolloverFrequency);
        context.setRolloverFrequencyTimeUnit(rolloverFrequencyTimeUnit);
        return this;
    }

    public EmbeddedDatabaseManagerBuilder numberOfAttemptedRetries(final int numberOfAttemptedRetries) {
        context.setNumberOfAttemptedRetries(numberOfAttemptedRetries);
        return this;
    }

    public EmbeddedDatabaseManagerBuilder backupLocation(final String backupLocation) {
        context.setBackupLocation(backupLocation);
        return this;
    }

    public EmbeddedDatabaseManagerBuilder addTable(final String name, final String definition) {
        return addTable(name, definition, RolloverStrategy.keep());
    }

    public EmbeddedDatabaseManagerBuilder addTable(final String name, final String definition, final RolloverStrategy rolloverStrategy) {
        context.addTableDefinition(new ManagedTableDefinition(name, definition, rolloverStrategy));
        return this;
    }

    public DatabaseManager build() {
        if (context.getLockAttemptTime() <= 0) {
            throw new IllegalArgumentException("Lock attempt time must be bigger than 0");
        }

        Objects.requireNonNull(context.getLockAttemptTimeUnit(), "Lock attempt time unit must be specified");

        if (context.getRolloverFrequency() <= 0) {
            throw new IllegalArgumentException("Rollover frequency must be bigger than 0");
        }

        Objects.requireNonNull(context.getRolloverFrequencyTimeUnit(), "Rollover frequency unit must be specified");

        if (context.getNumberOfAttemptedRetries() < 1) {
            throw new IllegalArgumentException("Number of attempted retries must be at least 1");
        }

        if (context.getTableDefinitions().size() < 1) {
            throw new IllegalArgumentException("There must be at least on table speficied");
        }

        if (context.getBackupLocation() == null) {
            context.setBackupLocation(new File(context.getPersistLocationAsFile().getParentFile(), "questDbBackup").getAbsolutePath());
        }

        final DatabaseManager result = new EmbeddedDatabaseManager(context);
        result.init();
        return result;
    }

    public static EmbeddedDatabaseManagerBuilder builder(final String persistPath) {
        return new EmbeddedDatabaseManagerBuilder(persistPath);
    }
}
