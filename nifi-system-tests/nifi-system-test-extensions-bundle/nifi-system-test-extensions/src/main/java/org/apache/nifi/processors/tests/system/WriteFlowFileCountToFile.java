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

import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.components.ClassloaderIsolationKeyProvider;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.nifi.expression.ExpressionLanguageScope.NONE;
import static org.apache.nifi.processor.util.StandardValidators.NON_EMPTY_VALIDATOR;

@RequiresInstanceClassLoading(cloneAncestorResources = true)
public class WriteFlowFileCountToFile extends AbstractProcessor implements ClassloaderIsolationKeyProvider {
    private static final AtomicLong counter = new AtomicLong(0L);

    static final PropertyDescriptor ISOLATION_KEY = new Builder()
        .name("Isolation Key")
        .displayName("Isolation Key")
        .description("The key to use as the ClassLoader Isolation Key")
        .required(true)
        .addValidator(NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(NONE)
        .build();

    static final PropertyDescriptor FILE_TO_WRITE = new Builder()
        .name("File to Write")
        .displayName("File to Write")
        .description("File to write the counts to")
        .required(true)
        .addValidator(NON_EMPTY_VALIDATOR)
        .defaultValue("counts.txt")
        .build();

    static final PropertyDescriptor CLASS_TO_CREATE = new Builder()
        .name("Class to Create")
        .displayName("Class to Create")
        .description("If specified, each iteration of #onTrigger will create an instance of this class in order to test ClassLoader behavior. If unable to create the object, the FlowFile will be " +
            "routed to failure")
        .required(false)
        .addValidator(NON_EMPTY_VALIDATOR)
        .build();

    private final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .build();
    private final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .autoTerminateDefault(true)
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(ISOLATION_KEY, FILE_TO_WRITE, CLASS_TO_CREATE);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE));
    }

    @Override
    public String getClassloaderIsolationKey(final PropertyContext context) {
        return context.getProperty(ISOLATION_KEY).getValue();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String className = context.getProperty(CLASS_TO_CREATE).getValue();
        if (className != null) {
            try {
                Class.forName(className, true, Thread.currentThread().getContextClassLoader());
            } catch (final ClassNotFoundException e) {
                getLogger().error("Failed to load class {} for {}; routing to failure", className, flowFile);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
        }

        final long counterValue = counter.incrementAndGet();
        final byte[] fileContents = String.valueOf(counterValue).getBytes(StandardCharsets.UTF_8);

        final File file = new File(context.getProperty(FILE_TO_WRITE).getValue());
        try (final OutputStream fos = new FileOutputStream(file)) {
            fos.write(fileContents);
        } catch (Exception e) {
            throw new ProcessException(e);
        }

        session.transfer(flowFile, REL_SUCCESS);
    }

}
