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

package org.apache.nifi.controller;

import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.attribute.expression.language.VariableImpact;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.parameter.ExpressionLanguageAgnosticParameterParser;
import org.apache.nifi.parameter.ExpressionLanguageAwareParameterParser;
import org.apache.nifi.parameter.ParameterParser;
import org.apache.nifi.parameter.ParameterReference;
import org.apache.nifi.parameter.ParameterTokenList;
import org.apache.nifi.util.CharacterFilterUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PropertyConfigurationMapper {
    private final ParameterParser elAgnosticParameterParser = new ExpressionLanguageAgnosticParameterParser();
    private final ParameterParser elAwareParameterParser = new ExpressionLanguageAwareParameterParser();

    public Map<String, PropertyConfiguration> mapRawPropertyValuesToPropertyConfiguration(final ComponentNode componentNode, final Map<String, String> rawPropertyValues) {
        final Map<String, PropertyConfiguration> configurationMap = new LinkedHashMap<>();

        for (final Map.Entry<String, String> entry : rawPropertyValues.entrySet()) {
            final String propertyName = entry.getKey();
            final String propertyValue = entry.getValue();
            final PropertyDescriptor propertyDescriptor = componentNode.getPropertyDescriptor(propertyName);
            final PropertyConfiguration configuration = mapRawPropertyValuesToPropertyConfiguration(propertyDescriptor, propertyValue);
            configurationMap.put(propertyName, configuration);
        }

        return configurationMap;
    }

    public PropertyConfiguration mapRawPropertyValuesToPropertyConfiguration(final PropertyDescriptor propertyDescriptor, final String propertyValue) {
        final String updatedValue = CharacterFilterUtils.filterInvalidXmlCharacters(propertyValue);

        // Use the EL-Agnostic Parameter Parser to gather the list of referenced Parameters. We do this because we want to to keep track of which parameters
        // are referenced, regardless of whether or not they are referenced from within an EL Expression. However, we also will need to derive a different ParameterTokenList
        // that we can provide to the PropertyConfiguration, so that when compiling the Expression Language Expressions, we are able to keep the Parameter Reference within
        // the Expression's text.
        final ParameterTokenList updatedValueReferences = elAgnosticParameterParser.parseTokens(updatedValue);
        final List<ParameterReference> parameterReferences = updatedValueReferences.toReferenceList();

        final boolean supportsEL = propertyDescriptor.isExpressionLanguageSupported();
        if (supportsEL) {
            final VariableImpact variableImpact = Query.prepare(propertyValue).getVariableImpact();
            return new PropertyConfiguration(updatedValue, elAwareParameterParser.parseTokens(updatedValue), parameterReferences, variableImpact);
        } else {
            return new PropertyConfiguration(updatedValue, updatedValueReferences, parameterReferences, VariableImpact.NEVER_IMPACTED);
        }

    }

}
