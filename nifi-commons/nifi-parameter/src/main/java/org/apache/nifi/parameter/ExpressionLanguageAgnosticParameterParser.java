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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ExpressionLanguageAgnosticParameterParser extends AbstractParameterParser {
    private static final Logger logger = LoggerFactory.getLogger(ExpressionLanguageAgnosticParameterParser.class);

    @Override
    public ParameterTokenList parseTokens(final String input) {
        if (input == null || input.isEmpty()) {
            return new StandardParameterTokenList(input, Collections.emptyList());
        }

        final List<ParameterToken> references = new ArrayList<>();
        int sequentialStartTags = 0;

        for (int i=0; i < input.length(); i++) {
            final char c = input.charAt(i);

            switch (c) {
                case START_TAG:
                    // If last character was a # character, then the previous character along with this character
                    // represent an escaped literal # character. Otherwise, this character potentially represents
                    // the start of a Parameter Reference.
                    sequentialStartTags++;
                    break;
                case OPEN_BRACE:
                    if (sequentialStartTags > 0) {
                        final ParameterToken token = parseParameterToken(input, i, sequentialStartTags, references);

                        // If we found a reference, skip 'i' to the end of the reference, since there can't be any other references before that point.
                        if (token != null) {
                            i = token.getEndOffset();
                        }
                    }

                    break;
                default:
                    break;
            }

            if (c != START_TAG) {
                sequentialStartTags = 0;
            }
        }

        return new StandardParameterTokenList(input, references);
    }
}
