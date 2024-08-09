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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

final class SimpleEmbeddedDatabaseManagerContext implements EmbeddedDatabaseManagerContext {
    final private Set<ManagedTableDefinition> tableDefinitions = new HashSet<>();

    private String persistLocation;
    private String backupLocation;
    private int numberOfAttemptedRetries;
    private Duration lockAttemptDuration;
    private Duration rolloverFrequnecyDuration;

    @Override
    public String getPersistLocation() {
        return persistLocation;
    }

    @Override
    public Path getPersistLocationAsPath() {
        return Paths.get(getPersistLocation());
    }

    @Override
    public String getBackupLocation() {
        return backupLocation;
    }

    @Override
    public Path getBackupLocationAsPath() {
        return Paths.get(getBackupLocation());
    }

    @Override
    public int getNumberOfAttemptedRetries() {
        return numberOfAttemptedRetries;
    }

    @Override
    public Duration getLockAttemptTime() {
        return lockAttemptDuration;
    }

    @Override
    public Duration getRolloverFrequency() {
        return rolloverFrequnecyDuration;
    }

    @Override
    public Set<ManagedTableDefinition> getTableDefinitions() {
        return tableDefinitions;
    }

    void setPersistLocation(final String persistLocation) {
        this.persistLocation = persistLocation;
    }

    public void setBackupLocation(final String backupLocation) {
        this.backupLocation = backupLocation;
    }

    void setNumberOfAttemptedRetries(final int numberOfAttemptedRetries) {
        this.numberOfAttemptedRetries = numberOfAttemptedRetries;
    }

    void setLockAttemptDuration(final Duration lockAttemptDuration) {
        this.lockAttemptDuration = lockAttemptDuration;
    }

    void setRolloverFrequencyDuration(final Duration rolloverFreqencyDuration) {
        this.rolloverFrequnecyDuration = rolloverFreqencyDuration;
    }

    void addTableDefinition(final ManagedTableDefinition tableDefinition) {
        tableDefinitions.add(tableDefinition);
    }

    @Override
    public String toString() {
        return "SimpleEmbeddedDatabaseManagerContext{" +
                "tableDefinitions=" + tableDefinitions +
                ", persistLocation='" + persistLocation + '\'' +
                ", backupLocation='" + backupLocation + '\'' +
                ", numberOfAttemptedRetries=" + numberOfAttemptedRetries +
                ", lockAttemptDuration=" + lockAttemptDuration +
                ", rolloverFrequnecyDuration=" + rolloverFrequnecyDuration +
                '}';
    }
}
