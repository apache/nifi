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

package org.apache.nifi.hbase;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

public interface VisibilityFetchSupport {
    PropertyDescriptor AUTHORIZATIONS = new PropertyDescriptor.Builder()
        .name("hbase-fetch-row-authorizations")
        .displayName("Authorizations")
        .description("The list of authorizations to pass to the scanner. This will be ignored if cell visibility labels are not in use.")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(Validator.VALID)
        .build();

    default List<String> getAuthorizations(ProcessContext context, FlowFile flowFile) {
        final String authorizationString = context.getProperty(AUTHORIZATIONS).isSet()
            ? context.getProperty(AUTHORIZATIONS).evaluateAttributeExpressions(flowFile).getValue().trim()
            : "";
        List<String> authorizations = new ArrayList<>();
        if (!StringUtils.isBlank(authorizationString)) {
            String[] parts = authorizationString.split(",");
            for (String part : parts) {
                authorizations.add(part.trim());
            }
        }

        return authorizations;
    }
}
