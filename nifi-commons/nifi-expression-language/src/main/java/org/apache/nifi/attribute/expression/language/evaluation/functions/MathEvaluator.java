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
import org.apache.nifi.attribute.expression.language.evaluation.NumberEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.NumberQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class MathEvaluator extends NumberEvaluator {

    private final Evaluator<Number> subject;
    private final Evaluator<String> methodName;
    private final Evaluator<Number> optionalArg;

    public MathEvaluator(final Evaluator<Number> subject, final Evaluator<String> methodName, final Evaluator<Number> optionalArg) {
        this.subject = subject;
        this.methodName = methodName;
        this.optionalArg = optionalArg;
    }

    @Override
    public QueryResult<Number> evaluate(final EvaluationContext evaluationContext) {
        final String methodNamedValue = methodName.evaluate(evaluationContext).getValue();
        if (methodNamedValue == null) {
            return new NumberQueryResult(null);
        }

        final Number subjectValue;
        if(subject != null) {
            subjectValue = subject.evaluate(evaluationContext).getValue();
            if(subjectValue == null){
                return new NumberQueryResult(null);
            }
        } else {
            subjectValue = null;
        }

        final Number optionalArgValue;
        if(optionalArg != null) {
            optionalArgValue = optionalArg.evaluate(evaluationContext).getValue();

            if(optionalArgValue == null) {
                return new NumberQueryResult(null);
            }
        } else {
            optionalArgValue = null;
        }

        try {
            Number executionValue = null;

            if (subjectValue == null){
                Method method;
                try {
                    method = Math.class.getMethod(methodNamedValue);
                } catch (NoSuchMethodException subjectlessNoMethodException) {
                    throw new AttributeExpressionLanguageException("Cannot evaluate 'math' function because no subjectless method was found with the name:'" +
                            methodNamedValue + "'", subjectlessNoMethodException);
                }

                if(method == null) {
                    throw new AttributeExpressionLanguageException("Cannot evaluate 'math' function because no subjectless method was found with the name:'" + methodNamedValue + "'");
                }

                executionValue = (Number) method.invoke(null);

            } else if(optionalArg == null) {
                boolean subjectIsDecimal = subjectValue instanceof Double;
                Method method;
                try {
                    method = Math.class.getMethod(methodNamedValue, subjectIsDecimal ? double.class : long.class);
                } catch (NoSuchMethodException noOptionalNoMethodException){
                    throw new AttributeExpressionLanguageException("Cannot evaluate 'math' function because no method was found matching the passed parameters:" +
                            " name:'" + methodNamedValue + "', one argument of type: '" + (subjectIsDecimal ? "double" : "long")+"'", noOptionalNoMethodException);
                }

                if(method == null) {
                    throw new AttributeExpressionLanguageException("Cannot evaluate 'math' function because no method was found matching the passed parameters:" +
                            " name:'" + methodNamedValue + "', one argument of type: '" + (subjectIsDecimal ? "double" : "long")+"'");
                }

                if (subjectIsDecimal){
                    executionValue = (Number) method.invoke(null, subjectValue.doubleValue());
                } else {
                    executionValue = (Number) method.invoke(null, subjectValue.longValue());
                }

            } else {
                boolean subjectIsDecimal = subjectValue instanceof Double;
                boolean optionalArgIsDecimal = optionalArgValue instanceof Double;
                Method method;
                boolean convertOptionalToInt = false;

                try {
                    method = Math.class.getMethod(methodNamedValue, subjectIsDecimal ? double.class : long.class, optionalArgIsDecimal ? double.class : long.class);
                } catch (NoSuchMethodException withOptionalNoMethodException) {

                    if (!optionalArgIsDecimal) {
                        try {
                            method = Math.class.getMethod(methodNamedValue, subjectIsDecimal ? double.class : long.class, int.class);
                        } catch (NoSuchMethodException withOptionalInnerNoMethodException) {
                            throw new AttributeExpressionLanguageException("Cannot evaluate 'math' function because no method was found matching the passed parameters: " + "name:'" +
                                    methodNamedValue + "', first argument type: '" + (subjectIsDecimal ? "double" : "long") + "', second argument type:  'long'", withOptionalInnerNoMethodException);
                        }
                        convertOptionalToInt = true;

                    } else {
                        throw new AttributeExpressionLanguageException("Cannot evaluate 'math' function because no method was found matching the passed parameters: " + "name:'" +
                                methodNamedValue + "', first argument type: '" + (subjectIsDecimal ? "double" : "long") + "', second argument type:  'double'", withOptionalNoMethodException);
                    }
                }

                if(method == null) {
                    throw new AttributeExpressionLanguageException("Cannot evaluate 'math' function because no method was found matching the passed parameters: " +
                            "name:'" + methodNamedValue + "', first argument type: '" + (subjectIsDecimal ? "double" : "long") + "', second argument type:  '"
                            + (optionalArgIsDecimal ? "double" : "long") + "'");
                }

                if (optionalArgIsDecimal) {
                    executionValue = (Number) method.invoke(null, subjectValue, optionalArgValue.doubleValue());
                } else {
                    if (convertOptionalToInt) {
                        executionValue = (Number) method.invoke(null, subjectValue, optionalArgValue.intValue());
                    } else {
                        executionValue = (Number) method.invoke(null, subjectValue, optionalArgValue.longValue());
                    }
                }
            }

            return new NumberQueryResult(executionValue);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new AttributeExpressionLanguageException("Unable to calculate math function value", e);
        }
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }

}
