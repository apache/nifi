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

package org.apache.nifi.stateless.parameter;

import java.util.ArrayList;
import java.util.List;

public class CompositeParameterProvider extends AbstractParameterProvider implements ParameterProvider {
    private final List<ParameterProvider> parameterProviders;

    public CompositeParameterProvider(final List<ParameterProvider> providers) {
        this.parameterProviders = new ArrayList<>(providers);
    }

    @Override
    public String getParameterValue(final String contextName, final String parameterName) {
        for (final ParameterProvider provider : parameterProviders) {
            if (!provider.isParameterDefined(contextName, parameterName)) {
                continue;
            }

            final String value = provider.getParameterValue(contextName, parameterName);
            if (value != null) {
                return value;
            }
        }

        return null;
    }

    @Override
    public boolean isParameterDefined(final String contextName, final String parameterName) {
        for (final ParameterProvider provider : parameterProviders) {
            final boolean defined = provider.isParameterDefined(contextName, parameterName);
            if (defined) {
                return true;
            }
        }

        return false;
    }
}
