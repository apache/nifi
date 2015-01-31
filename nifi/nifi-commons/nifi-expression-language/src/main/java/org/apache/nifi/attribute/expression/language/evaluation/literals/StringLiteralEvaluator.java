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
package org.apache.nifi.attribute.expression.language.evaluation.literals;

import java.util.Map;

import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;

public class StringLiteralEvaluator extends StringEvaluator {

    private final String value;

    public StringLiteralEvaluator(final String value) {
        // need to escape characters after backslashes
        final StringBuilder sb = new StringBuilder();
        boolean lastCharIsBackslash = false;
        for (int i = 0; i < value.length(); i++) {
            final char c = value.charAt(i);

            if (lastCharIsBackslash) {
                switch (c) {
                    case 'n':
                        sb.append("\n");
                        break;
                    case 'r':
                        sb.append("\r");
                        break;
                    case '\\':
                        sb.append("\\");
                        break;
                    case 't':
                        sb.append("\\t");
                        break;
                    default:
                        sb.append("\\").append(c);
                        break;
                }

                lastCharIsBackslash = false;
            } else if (c == '\\') {
                lastCharIsBackslash = true;
            } else {
                sb.append(c);
            }
        }

        this.value = sb.toString();
    }

    @Override
    public QueryResult<String> evaluate(final Map<String, String> attributes) {
        return new StringQueryResult(value);
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return null;
    }

}
