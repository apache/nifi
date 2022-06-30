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
package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("A simple version of ReplaceText that is intended to replace the contents of a flowfile with a " +
        "user-defined block of text.")
@Tags({ "text", "replace", "write" })
public class WriteText extends AbstractProcessor {
    public static PropertyDescriptor TEXT = new PropertyDescriptor.Builder()
            .name("write-text-text-field")
            .displayName("Replacement text")
            .description("This is the text that will overwrite the input flowfile's content.")
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("When processing succeeds, flowfiles go here")
            .build();
    public static Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("When an error occurs, the input flowfile is routed here.")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(TEXT);
    }

    @Override
    public Set<Relationship> getRelationships() {
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile input = session.get();
        if (input == null) {
            return;
        }

        String body = context.getProperty(TEXT).evaluateAttributeExpressions(input).getValue();
        boolean exception = false;
        try (OutputStream os = session.write(input)) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("Replaced flowfile body with {}", body);
            }
        } catch (IOException e) {
            getLogger().error("Error replacing flowfile content", e);
            exception = true;
        } finally {
            if (exception) {
                session.transfer(input, REL_FAILURE);
            } else {
                session.getProvenanceReporter().modifyContent(input);
                session.transfer(input, REL_SUCCESS);
            }
        }
    }
}
