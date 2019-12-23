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

import java.util.Optional;

public class StandardParameterReference implements ParameterReference {
    private final String parameterName;
    private final int startOffset;
    private final int endOffset;
    private final String referenceText;

    public StandardParameterReference(final String parameterName, final int startOffset, final int endOffset, final String referenceText) {
        this.parameterName = parameterName;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.referenceText = referenceText;
    }

    @Override
    public String getParameterName() {
        return parameterName;
    }

    @Override
    public int getStartOffset() {
        return startOffset;
    }

    @Override
    public int getEndOffset() {
        return endOffset;
    }

    @Override
    public String getText() {
        return referenceText;
    }

    @Override
    public boolean isEscapeSequence() {
        return false;
    }

    @Override
    public boolean isParameterReference() {
        return true;
    }

    @Override
    public String getValue(final ParameterLookup parameterLookup) {
        if (parameterLookup == null) {
            return referenceText;
        }

        final Optional<Parameter> parameter = parameterLookup.getParameter(parameterName);
        return parameter.map(Parameter::getValue).orElse(referenceText);
    }
}
