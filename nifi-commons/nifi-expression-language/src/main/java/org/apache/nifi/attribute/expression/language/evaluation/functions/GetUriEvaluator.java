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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.attribute.expression.language.EvaluationContext;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class GetUriEvaluator extends StringEvaluator {

    private final Evaluator<String>[] uriArgs;

    public GetUriEvaluator(Evaluator<String>[] uriArgs) {
        this.uriArgs = uriArgs;
    }

    @Override
    public QueryResult<String> evaluate(EvaluationContext evaluationContext) {
        List<String> args = Arrays.stream(uriArgs)
                .map(uriArg -> uriArg.evaluate(evaluationContext).getValue())
                .map(string -> StringUtils.isBlank(string) ? null : string)
                .collect(Collectors.toList());

        try {
            switch (args.size()) {
                case 3:
                    return new StringQueryResult(new URI(args.get(0), args.get(1), args.get(2)).toString());
                case 4:
                    return new StringQueryResult(new URI(args.get(0), args.get(1), args.get(2), args.get(3)).toString());
                case 5:
                    return new StringQueryResult(new URI(args.get(0), args.get(1), args.get(2), args.get(3), args.get(4)).toString());
                case 7:
                    return new StringQueryResult(new URI(args.get(0), args.get(1), args.get(2),
                            Integer.parseInt(args.get(3)), args.get(4), args.get(5), args.get(6)).toString());
                default:
                    throw new AttributeExpressionLanguageException("Could not evaluate 'getUri' function with " + args.size() + " argument(s)");
            }
        } catch (NumberFormatException nfe) {
            throw new AttributeExpressionLanguageException("Could not evaluate 'getUri' function with argument '"
                    + args.get(3) + "' which is not a number", nfe);
        } catch (URISyntaxException e) {
            args = args.stream()
                    .map(arg -> arg == null ? "''" : arg)
                    .collect(Collectors.toList());
            throw new AttributeExpressionLanguageException("Could not evaluate 'getUri' function with argument(s) " + args, e);
        }
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return null;
    }
}
