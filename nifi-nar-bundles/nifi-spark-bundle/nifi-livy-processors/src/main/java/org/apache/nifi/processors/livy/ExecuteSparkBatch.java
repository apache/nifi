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
package org.apache.nifi.processors.livy;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.api.livy.LivyBatch;
import org.apache.nifi.controller.api.livy.LivyBatchService;
import org.apache.nifi.controller.api.livy.LivySessionService;
import org.apache.nifi.controller.api.livy.exception.SessionManagerException;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@Tags({"spark", "livy", "http", "execute"})
@CapabilityDescription("Execute Spark Code over a Livy-managed HTTP session as a Spark Batch job.")
public class ExecuteSparkBatch extends AbstractProcessor {
    public static final PropertyDescriptor LIVY_CONTROLLER_SERVICE = new PropertyDescriptor.Builder()
            .name("exec-spark-batch-livy-controller-service")
            .displayName("Livy Controller Service")
            .description("The controller service to use for submitting Livy-managed batch jobs.")
            .required(true)
            .identifiesControllerService(LivyBatchService.class)
            .build();

    public static final PropertyDescriptor CODE_PATH = new PropertyDescriptor.Builder()
            .name("exec-spark-code-file-path")
            .displayName("Code File")
            .description("File containing the application to execute. " +
                    "For HDFS locations use the format hdfs:///path/to/examples.jar")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor CLASS_NAME = new PropertyDescriptor.Builder()
            .name("exec-spark-code-class-name")
            .displayName("Class Name")
            .description("For JVM applications, the name of the class to execute.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor CODE_ARGS = new PropertyDescriptor.Builder()
            .name("exec-spark-code-args")
            .displayName("Args")
            .description("The comma delimited list of arguments to pass to the file to be executed.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor STATUS_CHECK_INTERVAL = new PropertyDescriptor.Builder()
            .name("exec-spark-iactive-status-check-interval")
            .displayName("Status Check Interval")
            .description("The amount of time to wait between checking the status of an operation.")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("1 sec")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are successfully processed are sent to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed to this relationship when they cannot be parsed or failed to run")
            .build();

    private volatile List<PropertyDescriptor> properties;
    private volatile Set<Relationship> relationships;

    @Override
    public void init(final ProcessorInitializationContext context) {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(LIVY_CONTROLLER_SERVICE);
        properties.add(CODE_PATH);
        properties.add(CLASS_NAME);
        properties.add(CODE_ARGS);
        properties.add(STATUS_CHECK_INTERVAL);
        this.properties = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog log = getLogger();
        final LivyBatchService livyBatchService = context.getProperty(LIVY_CONTROLLER_SERVICE).asControllerService(LivyBatchService.class);

        final String codePath = context.getProperty(CODE_PATH).evaluateAttributeExpressions(flowFile).getValue();
        final String className = context.getProperty(CLASS_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String codeArgs = context.getProperty(CODE_ARGS).evaluateAttributeExpressions(flowFile).getValue();

        final long statusCheckInterval = context.getProperty(STATUS_CHECK_INTERVAL).evaluateAttributeExpressions(flowFile).asTimePeriod(TimeUnit.MILLISECONDS);

        try {
            final Integer batchId = livyBatchService.submitBatch(codePath, className, codeArgs);
            String jobState = livyBatchService.getBatchStatus(batchId);

            log.debug("executeSparkBatch() Job status is: " + jobState + ". Waiting for job to complete...");

            flowFile = session.putAttribute(flowFile, "livy.batchid", Integer.toString(batchId));

            while (!jobState.equalsIgnoreCase("error") &&
                    !jobState.equalsIgnoreCase("dead") &&
                    !jobState.equalsIgnoreCase("killed") &&
                    !jobState.equalsIgnoreCase("success")) {
                log.debug("executeSparkBatch() Job status is: " + jobState + ". Waiting for job to complete...");
                Thread.sleep(statusCheckInterval);
                jobState = livyBatchService.getBatchStatus(batchId);
            }

            // Livy Batch mode is like Spark-Submit, so unlike Interactive mode there is no output to write to the FlowFile
            // We'll write the log output instead
            LivyBatch livyBatch = livyBatchService.getBatchSession(batchId);
            flowFile = session.putAttribute(flowFile, "livy.jobstate", livyBatch.state);

            if(livyBatch.log != null){
                flowFile = session.write(flowFile, out -> out.write(String.join("\n", livyBatch.log).getBytes()));
            }

            // Job has ended, handle error/cancel states
            if(jobState.equalsIgnoreCase("error") ||
                jobState.equalsIgnoreCase("dead") ||
                jobState.equalsIgnoreCase("killed")) {
                session.transfer(flowFile, REL_FAILURE);
            } else {
                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch (SessionManagerException|InterruptedException e){
            log.error("Job processing error: " + e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
