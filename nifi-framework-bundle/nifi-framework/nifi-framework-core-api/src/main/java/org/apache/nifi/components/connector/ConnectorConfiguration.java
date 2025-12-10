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
package org.apache.nifi.components.connector;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class ConnectorConfiguration {

    private final Set<NamedStepConfiguration> stepConfigurations;

    public ConnectorConfiguration(final Set<NamedStepConfiguration> stepConfigurations) {
        this.stepConfigurations = new HashSet<>(stepConfigurations);
    }

    public Set<NamedStepConfiguration> getNamedStepConfigurations() {
        return stepConfigurations;
    }

    @Override
    public boolean equals(final Object other) {
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final ConnectorConfiguration that = (ConnectorConfiguration) other;
        return Objects.equals(stepConfigurations, that.stepConfigurations);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(stepConfigurations);
    }

    @Override
    public String toString() {
        return "ConnectorConfiguration[" +
               "stepConfigurations=" + stepConfigurations +
               "]";
    }
}
