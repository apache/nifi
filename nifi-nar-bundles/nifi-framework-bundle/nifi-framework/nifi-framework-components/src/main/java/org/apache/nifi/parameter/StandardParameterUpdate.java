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

public class StandardParameterUpdate implements ParameterUpdate {
    private final String parameterName;
    private final String previousValue;
    private final String updatedValue;
    private final boolean sensitiveParameter;

    public StandardParameterUpdate(final String parameterName, final String previousValue, final String updatedValue, final boolean sensitiveParameter) {
        this.parameterName = parameterName;
        this.previousValue = previousValue;
        this.updatedValue = updatedValue;
        this.sensitiveParameter = sensitiveParameter;
    }

    @Override
    public String getParameterName() {
        return parameterName;
    }

    @Override
    public String getPreviousValue() {
        return previousValue;
    }

    @Override
    public String getUpdatedValue() {
        return updatedValue;
    }

    @Override
    public boolean isSensitive() {
        return sensitiveParameter;
    }

    @Override
    public String toString() {
        if (sensitiveParameter) {
            return "StandardParameterUpdate[parameterName=" + parameterName + ", sensitive=true]";
        } else {
            return "StandardParameterUpdate[parameterName=" + parameterName + ", sensitive=false, previous value='" + previousValue + "', updated value='" + updatedValue + "']";
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o){
            return true;
        }

        if (!(o instanceof StandardParameterUpdate)) {
            return false;
        }

        final StandardParameterUpdate that = (StandardParameterUpdate) o;
        return Objects.equals(parameterName, that.parameterName)
            && Objects.equals(previousValue, that.previousValue)
            && Objects.equals(updatedValue, that.updatedValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parameterName, previousValue, updatedValue);
    }
}
