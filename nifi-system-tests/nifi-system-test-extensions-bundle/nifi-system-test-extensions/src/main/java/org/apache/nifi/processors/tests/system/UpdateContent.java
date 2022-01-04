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
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class UpdateContent extends AbstractProcessor {

    static final PropertyDescriptor CONTENT = new Builder()
        .name("Content")
        .displayName("Content")
        .description("Content to set")
        .required(true)
        .addValidator(Validator.VALID)
        .defaultValue("Default Content")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();
    static final PropertyDescriptor UPDATE_STRATEGY = new Builder()
        .name("Update Strategy")
        .displayName("Update Strategy")
        .description("How to update the contents")
        .required(true)
        .allowableValues("Replace", "Append")
        .defaultValue("Replace")
        .build();

    private final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(CONTENT, UPDATE_STRATEGY);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String content = context.getProperty(CONTENT).evaluateAttributeExpressions(flowFile).getValue();
        final byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);

        final String strategy = context.getProperty(UPDATE_STRATEGY).getValue();
        if (strategy.equalsIgnoreCase("Replace")) {
            session.write(flowFile, out -> out.write(contentBytes));
        } else {
            session.append(flowFile, out -> out.write(contentBytes));
        }

        session.transfer(flowFile, REL_SUCCESS);
    }
}
