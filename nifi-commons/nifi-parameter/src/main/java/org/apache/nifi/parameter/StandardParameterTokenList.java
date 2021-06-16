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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StandardParameterTokenList implements ParameterTokenList {
    private final String input;
    private final List<ParameterToken> tokens;
    private final List<ParameterReference> referenceList;

    public StandardParameterTokenList(final String input, final List<ParameterToken> tokens) {
        this.input = input;
        this.tokens = tokens;
        this.referenceList = tokens.stream()
            .filter(ParameterToken::isParameterReference)
            .map(token -> (ParameterReference) token)
            .collect(Collectors.toList());
    }

    @Override
    public List<ParameterReference> toReferenceList() {
        return this.referenceList;
    }

    @Override
    public String substitute(final ParameterLookup parameterLookup) {
        if (input == null) {
            return null;
        }

        if (tokens.isEmpty()) {
            return input;
        }

        return substitute(reference -> reference.getValue(parameterLookup));
    }

    @Override
    public String escape() {
        if (input == null) {
            return null;
        }

        return substitute(reference -> {
            if (reference.isEscapeSequence()) {
                if (reference.getText().equals("##")) {
                    return "####";
                } else {
                    return "##" + reference.getText();
                }
            } else {
                return "#" + reference.getText();
            }
        });
    }

    private String substitute(final Function<ParameterToken, String> transform) {
        final StringBuilder sb = new StringBuilder();

        int lastEndOffset = -1;
        for (final ParameterToken token : tokens) {
            final int startOffset = token.getStartOffset();

            sb.append(input, lastEndOffset + 1, startOffset);
            sb.append(transform.apply(token));

            lastEndOffset = token.getEndOffset();
        }

        if (input.length() > lastEndOffset + 1) {
            sb.append(input, lastEndOffset + 1, input.length());
        }

        return sb.toString();
    }

    @Override
    public List<ParameterToken> toList() {
        return Collections.unmodifiableList(tokens);
    }

    @Override
    public Iterator<ParameterToken> iterator() {
        return tokens.iterator();
    }
}
