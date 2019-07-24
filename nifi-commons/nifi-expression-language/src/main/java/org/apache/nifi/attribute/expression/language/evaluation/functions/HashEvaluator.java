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


import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.nifi.attribute.expression.language.EvaluationContext;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;

public class HashEvaluator extends StringEvaluator {
    private static final String SUPPORTED_ALGORITHMS = String.join(", ", Security.getAlgorithms("MessageDigest"));

    private final Evaluator<String> algorithm;
    private final Evaluator<String> subject;

    public HashEvaluator(final Evaluator<String> subject, final Evaluator<String> algorithm) {
        this.subject = subject;
        this.algorithm = algorithm;
    }

    @Override
    public QueryResult<String> evaluate(EvaluationContext context) {
        final String subjectValue = subject.evaluate(context).getValue();
        if (subjectValue == null) {
            return new StringQueryResult(null);
        }

        final String algorithmValue = algorithm.evaluate(context).getValue();
        final MessageDigest digest = getDigest(algorithmValue);
        String encoded = new DigestUtils(digest).digestAsHex(subjectValue);
        return new StringQueryResult(encoded);
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }

    private MessageDigest getDigest(String algorithm){
        try {
            return MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new AttributeExpressionLanguageException("Invalid hash algorithm: " + algorithm + " not in set [" + SUPPORTED_ALGORITHMS + "]", e);
        }
    }
}
