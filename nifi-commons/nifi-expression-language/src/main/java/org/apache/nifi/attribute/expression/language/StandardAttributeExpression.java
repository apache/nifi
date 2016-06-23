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
package org.apache.nifi.attribute.expression.language;

import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.AttributeValueDecorator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.VariableRegistryUtils;

public class StandardAttributeExpression implements AttributeExpression {

    private final Query query;
    private final VariableRegistry variableRegistry;

    public StandardAttributeExpression(final Query query, final VariableRegistry variableRegistry) {
        this.query = query;
        this.variableRegistry = variableRegistry;
    }

    @Override
    public ResultType getResultType() {
        return query.getResultType();
    }

    @Override
    public String evaluate() throws ProcessException {
        return evaluate((AttributeValueDecorator) null);
    }

    @Override
    public String evaluate(final AttributeValueDecorator decorator) throws ProcessException {
        return evaluate(null, decorator);
    }

    @Override
    public String evaluate(final FlowFile flowFile) throws ProcessException {
        return evaluate(flowFile, null);
    }

    @Override
    public String evaluate(final FlowFile flowFile, final AttributeValueDecorator decorator) throws ProcessException {
        VariableRegistry flowFileRegistry = VariableRegistryUtils.createFlowVariableRegistry(variableRegistry,flowFile,null);
        final Object evaluationResult = query.evaluate(flowFileRegistry).getValue();
        if (evaluationResult == null) {
            return "";
        }

        String result = evaluationResult.toString();
        if (decorator != null) {
            result = decorator.decorate(result);
        }
        return Query.unescape(result);
    }
}
