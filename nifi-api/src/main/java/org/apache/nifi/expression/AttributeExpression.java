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
package org.apache.nifi.expression;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.registry.VariableRegistry;

/**
 * Defines a type of expression language statement that can be applied
 * parametrized by various attributes and properties as specified in each of
 * the method calls. AttributeExpression evaluations may be also backed by a
 * {@link VariableRegistry} used to substitute attributes and variables found in
 * the expression for which the registry has a value.
 */
public interface AttributeExpression {

    /**
     * @return Evaluates the expression without any additional attributes.
     * @throws ProcessException if unable to evaluate
     */
    String evaluate() throws ProcessException;

    /**
     * Evaluates the expression without additional attributes but enables the
     * expression result to be decorated before returning.
     *
     * @param decorator to execute on the resulting expression value
     * @return evaluated value
     * @throws ProcessException if failure in evaluation
     */
    String evaluate(AttributeValueDecorator decorator) throws ProcessException;

    /**
     * Evaluates the expression providing access to additional variables
     * including the flow file properties such as file size, identifier, etc..
     * and also all of the flow file attributes.
     *
     * @param flowFile to evaluate
     * @return evaluated value
     * @throws ProcessException if failure evaluating
     */
    String evaluate(FlowFile flowFile) throws ProcessException;

    /**
     * Evaluates the expression providing access to additional variables
     * including the flow file properties such as file size, identifier, etc..
     * and also all of the flow file attributes. The resulting value after
     * executing any variable substitution and expression evaluation is run
     * through the given decorator and returned.
     *
     * @param flowFile to evaluate
     * @param decorator for evaluation
     * @return evaluated value
     * @throws ProcessException if failed to evaluate
     */
    String evaluate(FlowFile flowFile, AttributeValueDecorator decorator) throws ProcessException;

    /**
     * @return the type that is returned by the Expression
     */
    ResultType getResultType();

    public static enum ResultType {

        STRING, BOOLEAN, WHOLE_NUMBER, DATE, DECIMAL, NUMBER;
    }
}
