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
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterLookup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestStandardSearchContext {

    private static final String SCRIPTS_DIR_PARAMETER = "scripts.dir";

    private static final String SECRET_PARAMETER = "secret";

    private static final String SECRET_VALUE = "s3cr3t";

    private static final PropertyDescriptor SCRIPT_FILE = new PropertyDescriptor.Builder()
            .name("Script File")
            .required(false)
            .addValidator(Validator.VALID)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    private static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .required(false)
            .sensitive(true)
            .addValidator(Validator.VALID)
            .build();

    @Mock
    private ProcessorNode processorNode;

    @Mock
    private ControllerServiceLookup controllerServiceLookup;

    private StandardSearchContext context;

    @BeforeEach
    void setContext() {
        final Map<String, Parameter> parameters = Map.of(
                SCRIPTS_DIR_PARAMETER, new Parameter.Builder().name(SCRIPTS_DIR_PARAMETER).value("/opt/scripts").sensitive(false).build(),
                SECRET_PARAMETER, new Parameter.Builder().name(SECRET_PARAMETER).value(SECRET_VALUE).sensitive(true).build()
        );
        when(processorNode.getParameterLookup()).thenReturn(new MapParameterLookup(parameters));

        context = new StandardSearchContext("search-me", processorNode, controllerServiceLookup);
    }

    @Test
    void getPropertyEvaluatedResolvesParameter() {
        when(processorNode.getRawPropertyValue(SCRIPT_FILE)).thenReturn("#{scripts.dir}/file.groovy");

        assertEquals("/opt/scripts/file.groovy", context.getProperty(SCRIPT_FILE).evaluateAttributeExpressions().getValue());
    }

    @Test
    void getPropertyValueRemainsRaw() {
        when(processorNode.getRawPropertyValue(SCRIPT_FILE)).thenReturn("#{scripts.dir}/file.groovy");

        assertEquals("#{scripts.dir}/file.groovy", context.getProperty(SCRIPT_FILE).getValue());
    }

    @Test
    void getPropertyEvaluatedExcludesSensitiveParameterForNonSensitiveProperty() {
        when(processorNode.getRawPropertyValue(SCRIPT_FILE)).thenReturn("#{secret}");

        final String evaluated = context.getProperty(SCRIPT_FILE).evaluateAttributeExpressions().getValue();

        assertFalse(evaluated != null && evaluated.contains(SECRET_VALUE));
    }

    @Test
    void getPropertyEvaluatedExcludesNonSensitiveParameterForSensitiveProperty() {
        when(processorNode.getRawPropertyValue(PASSWORD)).thenReturn("#{scripts.dir}");

        final String evaluated = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();

        assertFalse(evaluated != null && evaluated.contains("/opt/scripts"));
    }

    private record MapParameterLookup(Map<String, Parameter> parameters) implements ParameterLookup {
        @Override
        public Optional<Parameter> getParameter(final String parameterName) {
            return Optional.ofNullable(parameters.get(parameterName));
        }

        @Override
        public boolean isEmpty() {
            return parameters.isEmpty();
        }

        @Override
        public long getVersion() {
            return 0;
        }
    }
}
