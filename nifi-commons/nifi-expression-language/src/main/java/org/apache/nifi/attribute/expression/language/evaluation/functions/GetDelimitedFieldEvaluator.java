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

package org.apache.nifi.attribute.expression.language.evaluation.functions;

import org.apache.nifi.attribute.expression.language.EvaluationContext;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.literals.BooleanLiteralEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.literals.StringLiteralEvaluator;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;

public class GetDelimitedFieldEvaluator extends StringEvaluator {
    private final Evaluator<String> subjectEval;
    private final Evaluator<Long> indexEval;
    private final Evaluator<String> delimiterEval;
    private final Evaluator<String> quoteCharEval;
    private final Evaluator<String> escapeCharEval;
    private final Evaluator<Boolean> stripCharsEval;

    public GetDelimitedFieldEvaluator(final Evaluator<String> subject, final Evaluator<Long> index) {
        this(subject, index, new StringLiteralEvaluator(","));
    }

    public GetDelimitedFieldEvaluator(final Evaluator<String> subject, final Evaluator<Long> index, final Evaluator<String> delimiter) {
        this(subject, index, delimiter, new StringLiteralEvaluator("\""));
    }

    public GetDelimitedFieldEvaluator(final Evaluator<String> subject, final Evaluator<Long> index, final Evaluator<String> delimiter,
        final Evaluator<String> quoteChar) {
        this(subject, index, delimiter, quoteChar, new StringLiteralEvaluator("\\\\"));
    }

    public GetDelimitedFieldEvaluator(final Evaluator<String> subject, final Evaluator<Long> index, final Evaluator<String> delimiter,
        final Evaluator<String> quoteChar, final Evaluator<String> escapeChar) {
        this(subject, index, delimiter, quoteChar, escapeChar, new BooleanLiteralEvaluator(false));
    }

    public GetDelimitedFieldEvaluator(final Evaluator<String> subject, final Evaluator<Long> index, final Evaluator<String> delimiter,
        final Evaluator<String> quoteChar, final Evaluator<String> escapeChar, final Evaluator<Boolean> stripChars) {
        this.subjectEval = subject;
        this.indexEval = index;
        this.delimiterEval = delimiter;
        this.quoteCharEval = quoteChar;
        this.escapeCharEval = escapeChar;
        this.stripCharsEval = stripChars;
    }

    @Override
    public QueryResult<String> evaluate(final EvaluationContext evaluationContext) {
        final String subject = subjectEval.evaluate(evaluationContext).getValue();
        if (subject == null || subject.isEmpty()) {
            return new StringQueryResult("");
        }

        final Long index = indexEval.evaluate(evaluationContext).getValue();
        if (index == null) {
            throw new AttributeExpressionLanguageException("Cannot evaluate getDelimitedField function because the index (which field to obtain) was not specified");
        }
        if (index < 1) {
            return new StringQueryResult("");
        }

        final String delimiter = delimiterEval.evaluate(evaluationContext).getValue();
        if (delimiter == null || delimiter.isEmpty()) {
            throw new AttributeExpressionLanguageException("Cannot evaluate getDelimitedField function because the delimiter was not specified");
        } else if (delimiter.length() > 1) {
            throw new AttributeExpressionLanguageException("Cannot evaluate getDelimitedField function because the delimiter evaluated to \"" + delimiter
                + "\", but only a single character is allowed.");
        }

        final String quoteString = quoteCharEval.evaluate(evaluationContext).getValue();
        if (quoteString == null || quoteString.isEmpty()) {
            throw new AttributeExpressionLanguageException("Cannot evaluate getDelimitedField function because the quote character "
                + "(which character is used to enclose values that contain the delimiter) was not specified");
        } else if (quoteString.length() > 1) {
            throw new AttributeExpressionLanguageException("Cannot evaluate getDelimitedField function because the quote character "
                + "(which character is used to enclose values that contain the delimiter) evaluated to \"" + quoteString + "\", but only a single character is allowed.");
        }

        final String escapeString = escapeCharEval.evaluate(evaluationContext).getValue();
        if (escapeString == null || escapeString.isEmpty()) {
            throw new AttributeExpressionLanguageException("Cannot evaluate getDelimitedField function because the escape character "
                + "(which character is used to escape the quote character or delimiter) was not specified");
        } else if (escapeString.length() > 1) {
            throw new AttributeExpressionLanguageException("Cannot evaluate getDelimitedField function because the escape character "
                + "(which character is used to escape the quote character or delimiter) evaluated to \"" + escapeString + "\", but only a single character is allowed.");
        }

        Boolean stripChars = stripCharsEval.evaluate(evaluationContext).getValue();
        if (stripChars == null) {
            stripChars = Boolean.FALSE;
        }

        final char quoteChar = quoteString.charAt(0);
        final char delimiterChar = delimiter.charAt(0);
        final char escapeChar = escapeString.charAt(0);

        // ensure that quoteChar, delimiterChar, escapeChar are all different.
        if (quoteChar == delimiterChar) {
            throw new AttributeExpressionLanguageException("Cannot evaluate getDelimitedField function because the quote character and the delimiter are the same");
        }
        if (quoteChar == escapeChar) {
            throw new AttributeExpressionLanguageException("Cannot evaluate getDelimitedField function because the quote character and the escape character are the same");
        }
        if (delimiterChar == escapeChar) {
            throw new AttributeExpressionLanguageException("Cannot evaluate getDelimitedField function because the delimiter and the escape character are the same");
        }

        // Iterate through each character in the subject, trying to find the field index that we care about and extracting the chars from it.
        final StringBuilder fieldBuilder = new StringBuilder();
        final int desiredFieldIndex = index.intValue();
        final int numChars = subject.length();

        boolean inQuote = false;
        int curFieldIndex = 1;
        boolean lastCharIsEscape = false;
        for (int i = 0; i < numChars; i++) {
            final char c = subject.charAt(i);

            if (c == quoteChar && !lastCharIsEscape) {
                // we found a quote character that is not escaped. Flip the value of 'inQuote'
                inQuote = !inQuote;
                if (!stripChars && curFieldIndex == desiredFieldIndex) {
                    fieldBuilder.append(c);
                }
            } else if (c == delimiterChar && !lastCharIsEscape && !inQuote) {
                // We found a delimiter that is not escaped and we are not in quotes - or we ran out of characters so we consider this
                // the last character.
                final int indexJustFinished = curFieldIndex++;
                if (indexJustFinished == desiredFieldIndex) {
                    return new StringQueryResult(fieldBuilder.toString());
                }
            } else if (curFieldIndex == desiredFieldIndex) {
                if (c != escapeChar || !stripChars) {
                    fieldBuilder.append(c);
                }
            }

            lastCharIsEscape = (c == escapeChar) && !lastCharIsEscape;
        }

        if (curFieldIndex == desiredFieldIndex) {
            // we have run out of characters and we are on the desired field. Return the characters from this field.
            return new StringQueryResult(fieldBuilder.toString());
        }

        // We did not find enough fields. Return an empty string.
        return new StringQueryResult("");
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subjectEval;
    }

}
