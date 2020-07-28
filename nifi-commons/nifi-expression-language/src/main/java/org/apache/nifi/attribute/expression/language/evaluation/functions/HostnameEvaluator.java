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

import java.net.InetAddress;
import java.net.UnknownHostException;


public class HostnameEvaluator extends StringEvaluator {

    private final StringQueryResult hostname;

    public HostnameEvaluator(final boolean preferFQDN) throws UnknownHostException {
        String address;

        if (preferFQDN) {
            try {
                address = InetAddress.getLocalHost().getCanonicalHostName();
            } catch (final Exception e) {
                address = InetAddress.getLocalHost().getHostName();
            }
        } else {
            address = InetAddress.getLocalHost().getHostName();
        }

        hostname = new StringQueryResult(address);
    }

    @Override
    public QueryResult<String> evaluate(final EvaluationContext evaluationContext) {
        return hostname;
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return null;
    }

}
