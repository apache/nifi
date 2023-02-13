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

package org.apache.nifi.minifi.c2.command;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.expression.ExpressionLanguageCompiler;

public class AgentPropertyValidationContext implements ValidationContext {

        @Override
        public boolean isExpressionLanguageSupported(String propertyName) {
            return false;
        }

        @Override
        public Map<PropertyDescriptor, String> getProperties() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> getAllProperties() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ControllerServiceLookup getControllerServiceLookup() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ValidationContext getControllerServiceValidationContext(ControllerService controllerService) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExpressionLanguageCompiler newExpressionLanguageCompiler() {
            throw new UnsupportedOperationException();
        }

        @Override
        public PropertyValue newPropertyValue(String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getAnnotationData() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isValidationRequired(ControllerService service) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isExpressionLanguagePresent(String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getProcessGroupIdentifier() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<String> getReferencedParameters(String propertyName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isParameterDefined(String parameterName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isParameterSet(String parameterName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isDependencySatisfied(PropertyDescriptor propertyDescriptor, Function<String, PropertyDescriptor> propertyDescriptorLookup) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PropertyValue getProperty(PropertyDescriptor descriptor) {
            throw new UnsupportedOperationException();
        }
}
