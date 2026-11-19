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
package org.apache.nifi.web.controller;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestStandardSearchContext {

    private static final PropertyDescriptor SCRIPT_FILE = new PropertyDescriptor.Builder()
            .name("Script File")
            .required(false)
            .addValidator(Validator.VALID)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    @Mock
    private ProcessorNode processorNode;

    @Mock
    private ControllerServiceLookup controllerServiceLookup;

    @Test
    void getPropertyUsesEffectiveValueWithParametersResolved() {
        when(processorNode.getEffectivePropertyValues()).thenReturn(Map.of(SCRIPT_FILE, "/opt/scripts/file.groovy"));

        final StandardSearchContext context = new StandardSearchContext("search-me", processorNode, controllerServiceLookup);

        assertEquals("/opt/scripts/file.groovy", context.getProperty(SCRIPT_FILE).evaluateAttributeExpressions().getValue());
    }

    @Test
    void getPropertyKeepsUnresolvedParameterReferenceLiteral() {
        when(processorNode.getEffectivePropertyValues()).thenReturn(Map.of(SCRIPT_FILE, "#{scripts.dir}/file.groovy"));

        final StandardSearchContext context = new StandardSearchContext("search-me", processorNode, controllerServiceLookup);

        assertEquals("#{scripts.dir}/file.groovy", context.getProperty(SCRIPT_FILE).evaluateAttributeExpressions().getValue());
    }
}
