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

package org.apache.nifi.processors.tests.system;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class EvaluatePropertiesWithDifferentELScopes extends AbstractProcessor {
    static final PropertyDescriptor EVALUATE_FLOWFILE_CONTEXT = new PropertyDescriptor.Builder()
        .name("FlowFile Context")
        .description("The value of the property will be evaluated with FlowFile attributes")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();
    static final PropertyDescriptor EVALUATE_VARIABLE_REGISTRY_CONTEXT = new PropertyDescriptor.Builder()
        .name("Variable Registry Context")
        .description("The value of the property will be evaluated with only with the Variable Registry")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();
    static final PropertyDescriptor EVALUATE_NO_EL_CONTEXT = new PropertyDescriptor.Builder()
        .name("Expression Language Not Evaluated")
        .description("The value of the property will be evaluated without evaluating Expression Language")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .build();

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(EVALUATE_FLOWFILE_CONTEXT, EVALUATE_VARIABLE_REGISTRY_CONTEXT, EVALUATE_NO_EL_CONTEXT);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final Integer flowFileContext = context.getProperty(EVALUATE_FLOWFILE_CONTEXT).evaluateAttributeExpressions(flowFile).asInteger();
        final Integer variableRegistryContext = context.getProperty(EVALUATE_VARIABLE_REGISTRY_CONTEXT).evaluateAttributeExpressions().asInteger();
        final String noElContext = context.getProperty(EVALUATE_NO_EL_CONTEXT).getValue();

        if (flowFileContext != null) {
            session.adjustCounter("flowfile", flowFileContext, false);
        }

        if (variableRegistryContext != null) {
            session.adjustCounter("variable.registry", variableRegistryContext, false);
        }

        if (noElContext != null && noElContext.matches("[0-9]+")) {
            session.adjustCounter("no.el.evaluation", Integer.parseInt(noElContext), false);
        }

        session.transfer(flowFile, REL_SUCCESS);
    }
}
