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
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;

public class GetUriEvaluator extends StringEvaluator {

    private final List<Evaluator<String>> uriArgs;

    public GetUriEvaluator(List<Evaluator<String>> uriArgs) {
        this.uriArgs = uriArgs;
    }

    @Override
    public QueryResult<String> evaluate(EvaluationContext evaluationContext) {
        List<String> args = uriArgs.stream()
                .map(uriArg -> uriArg.evaluate(evaluationContext).getValue())
                .collect(Collectors.toList());

        try {
            if (args.size() == 7) {
                return new StringQueryResult(new URI(args.get(0), args.get(1), args.get(2),
                        Integer.parseInt(args.get(3)), args.get(4), args.get(5), args.get(6)).toString());
            }
            throw new AttributeExpressionLanguageException("Could not evaluate 'getUri' function with " + args.size() + " argument(s)");
        } catch (NumberFormatException nfe) {
            throw new AttributeExpressionLanguageException("Could not evaluate 'getUri' function with argument '"
                    + args.get(3) + "' which is not a number", nfe);
        } catch (URISyntaxException use) {
            throw new AttributeExpressionLanguageException("Could not evaluate 'getUri' function with argument(s) " + args, use);
        }
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return null;
    }
}
