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

import org.apache.nifi.attribute.expression.language.evaluation.DecimalEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.DecimalQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

public class MathEvaluator extends DecimalEvaluator {

    private final Evaluator<Double> subject;
    private final Evaluator<String> methodName;
    private final Evaluator<Double> optionalDecimal;

    public MathEvaluator(final Evaluator<Double> subject, final Evaluator<String> methodName, final Evaluator<Double> optionalDecimal) {
        this.subject = subject;
        this.methodName = methodName;
        this.optionalDecimal = optionalDecimal;
    }

    @Override
    public QueryResult<Double> evaluate(final Map<String, String> attributes) {
        final String methodNamedValue = methodName.evaluate(attributes).getValue();
        if (methodNamedValue == null) {
            return new DecimalQueryResult(null);
        }

        final Double subjectValue;
        if(subject != null) {
            subjectValue = subject.evaluate(attributes).getValue();
            if(subjectValue == null){
                throw new AttributeExpressionLanguageException("Cannot evaluate 'math' function because the subject evaluated to be null.");
            }
        } else {
            throw new AttributeExpressionLanguageException("Cannot evaluate 'math' function because the subject was null.");
        }

        final Double optionalDecimalValue;
        if(optionalDecimal != null) {
            optionalDecimalValue = optionalDecimal.evaluate(attributes).getValue();

            if(optionalDecimalValue == null) {
                throw new AttributeExpressionLanguageException("Cannot evaluate 'math' function because the second argument evaluated to be null. " +
                        "If a function with only one argument is desired to run (like 'sqrt') only pass pass the name of the method to 'math'.");
            }
        } else {
            optionalDecimalValue = null;
        }

        try {
            Double executionValue = null;

            if(optionalDecimal == null) {
                Method method = Math.class.getMethod(methodNamedValue, double.class);

                if(method == null) {
                    throw new AttributeExpressionLanguageException("Cannot evaluate 'math' function because no method was found matching the passed parameters:" +
                            " name:'"+methodNamedValue+"' and one double argument.");
                }

                executionValue = (Double) method.invoke(null, (double) subjectValue);

            } else {

                Method method = Math.class.getMethod(methodNamedValue, double.class, double.class);

                if(method == null) {
                    throw new AttributeExpressionLanguageException("Cannot evaluate 'math' function because no method was found matching the passed parameters: " +
                            "name:'"+methodNamedValue+"' and two double arguments.");
                }

                executionValue = (Double) method.invoke(null, (double) subjectValue, (double) optionalDecimalValue);
            }

            return new DecimalQueryResult(executionValue);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            return new DecimalQueryResult(null);
        }
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }

}
