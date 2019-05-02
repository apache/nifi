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

public class ExpressionLanguageAwareParameterParser extends AbstractParameterParser implements ParameterParser {
    private static final Logger logger = LoggerFactory.getLogger(ExpressionLanguageAwareParameterParser.class);
    private static final char DOLLAR_SIGN = '$';


    @Override
    public ParameterTokenList parseTokens(final String input) {
        if (input == null || input.isEmpty()) {
            return new StandardParameterTokenList(input, Collections.emptyList());
        }

        final List<ParameterToken> references = new ArrayList<>();

        int sequentialStartTags = 0;
        boolean oddDollarCount = false;
        char lastChar = 0;
        int embeddedElCount = 0;
        int expressionStart = -1;

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
                    if (oddDollarCount && lastChar == '$') {
                        if (embeddedElCount == 0) {
                            expressionStart = i - 1;
                        }
                    }

                    // Keep track of the number of opening curly braces that we are embedded within,
                    // if we are within an Expression. If we are outside of an Expression, we can just ignore
                    // curly braces. This allows us to ignore the first character if the value is something
                    // like: { ${abc} }
                    // However, we will count the curly braces if we have something like: ${ $${abc} }
                    if (expressionStart > -1) {
                        embeddedElCount++;
                        continue;
                    }


                    if (sequentialStartTags > 0) {
                        final ParameterToken token = parseParameterToken(input, i, sequentialStartTags, references);

                        // If we found a reference, skip 'i' to the end of the reference, since there can't be any other references before that point.
                        if (token != null) {
                            i = token.getEndOffset();
                        }
                    }

                    break;
                case CLOSE_BRACE:
                    if (embeddedElCount <= 0) {
                        continue;
                    }

                    if (--embeddedElCount == 0) {
                        expressionStart = -1;
                    }
                    break;
                case DOLLAR_SIGN:
                    oddDollarCount = !oddDollarCount;
                    break;
                default:
                    break;
            }

            if (c != START_TAG) {
                sequentialStartTags = 0;
            }

            lastChar = c;
        }

        logger.debug("For input {} found {} Parameter references: {}", input, references.size(), references);
        return new StandardParameterTokenList(input, references);
    }

}
