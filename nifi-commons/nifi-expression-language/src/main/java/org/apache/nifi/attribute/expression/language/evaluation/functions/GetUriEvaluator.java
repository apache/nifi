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
                final String scheme = args.get(0);
                final String userInfo = args.get(1);
                final String host = args.get(2);
                final int port = getPort(args.get(3));
                final String path = args.get(4);
                final String query = args.get(5);
                final String fragment = args.get(6);
                final URI uri = new URI(scheme, userInfo, host, port, path, query, fragment);
                return new StringQueryResult(uri.toString());
            }
            throw new AttributeExpressionLanguageException("Could not evaluate 'getUri' function with " + args.size() + " argument(s)");
        } catch (URISyntaxException use) {
            throw new AttributeExpressionLanguageException("Could not evaluate 'getUri' function with argument(s) " + args, use);
        }
    }

    private int getPort(String portArg) {
        try {
            return Integer.parseInt(portArg);
        } catch (NumberFormatException nfe) {
            throw new AttributeExpressionLanguageException("Could not evaluate 'getUri' function with argument '"
                    + portArg + "' which is not a number", nfe);
        }
    }
    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return null;
    }
}
