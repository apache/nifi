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

import java.util.Map;

import org.apache.nifi.expression.AttributeValueDecorator;

public class StringLiteralExpression implements Expression {
    private final String value;

    public StringLiteralExpression(final String value) {
        this.value = value;
    }

    @Override
    public String evaluate(Map<String, String> variables, AttributeValueDecorator decorator, Map<String, String> stateVariables) {
        return value;
    }
}
