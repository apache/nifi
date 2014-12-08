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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.nifi.attribute.expression.language.evaluation.DateEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;

public class FormatEvaluator extends StringEvaluator {

    private final DateEvaluator subject;
    private final StringEvaluator format;

    public FormatEvaluator(final DateEvaluator subject, final StringEvaluator format) {
        this.subject = subject;
        this.format = format;
    }

    @Override
    public QueryResult<String> evaluate(final Map<String, String> attributes) {
        final Date subjectValue = subject.evaluate(attributes).getValue();
        if (subjectValue == null) {
            return new StringQueryResult(null);
        }

        final QueryResult<String> formatResult = format.evaluate(attributes);
        final String format = formatResult.getValue();
        if (format == null) {
            return null;
        }

        return new StringQueryResult(new SimpleDateFormat(format).format(subjectValue));
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }

}
