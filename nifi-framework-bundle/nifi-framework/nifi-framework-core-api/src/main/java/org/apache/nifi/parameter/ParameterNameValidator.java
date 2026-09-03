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

import java.util.regex.Pattern;

public final class ParameterNameValidator {
    private static final Pattern VALID_PARAMETER_NAME_PATTERN = Pattern.compile("[A-Za-z0-9 ._\\-]+");

    private ParameterNameValidator() {
    }

    public static boolean isValid(final String parameterName) {
        return parameterName != null && VALID_PARAMETER_NAME_PATTERN.matcher(parameterName).matches();
    }

    public static void validate(final String parameterName) {
        if (!isValid(parameterName)) {
            throw new IllegalArgumentException("Request contains an illegal Parameter Name (" + parameterName
                    + "). Parameter names may only include letters, numbers, spaces, and the special characters .-_");
        }
    }
}
