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
package org.apache.nifi.parameter;

import java.util.Objects;

public class StandardParameterProviderConfiguration implements ParameterProviderConfiguration {

    private final String parameterProviderId;
    private final String parameterGroupName;
    private final boolean isSynchronized;

    public StandardParameterProviderConfiguration(final String parameterProviderId, final String parameterGroupName, final Boolean isSynchronized) {
        this.parameterProviderId = Objects.requireNonNull(parameterProviderId, "Parameter Provider ID is required");
        this.parameterGroupName = Objects.requireNonNull(parameterGroupName, "Parameter Group Name is required");
        this.isSynchronized = isSynchronized == null ? false : isSynchronized;
    }

    @Override
    public String getParameterProviderId() {
        return parameterProviderId;
    }

    @Override
    public String getParameterGroupName() {
        return parameterGroupName;
    }

    @Override
    public boolean isSynchronized() {
        return isSynchronized;
    }
}
