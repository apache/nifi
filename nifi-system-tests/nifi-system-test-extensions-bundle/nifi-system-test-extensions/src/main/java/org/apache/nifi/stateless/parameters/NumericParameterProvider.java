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

package org.apache.nifi.stateless.parameters;

import org.apache.nifi.stateless.parameter.AbstractParameterProvider;

import java.util.HashMap;
import java.util.Map;

public class NumericParameterProvider extends AbstractParameterProvider {
    private final Map<String, String> parameterValues = new HashMap<>();

    {
        parameterValues.put("zero", "0");
        parameterValues.put("one", "1");
        parameterValues.put("two", "2");
        parameterValues.put("three", "3");
        parameterValues.put("four", "4");
        parameterValues.put("five", "5");
        parameterValues.put("six", "6");
        parameterValues.put("seven", "7");
        parameterValues.put("eight", "8");
        parameterValues.put("nine", "9");
    }

    @Override
    public String getParameterValue(final String contextName, final String parameterName) {
        return parameterValues.get(parameterName);
    }

    @Override
    public boolean isParameterDefined(final String contextName, final String parameterName) {
        return parameterValues.containsKey(parameterName);
    }
}
