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
package org.apache.nifi.processors.aws.cloudwatch;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricDataResult;


@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"amazon", "aws", "cloudwatch", "metrics", "put", "publish"})
@CapabilityDescription("Publishes metrics to Amazon CloudWatch")
public class PutCloudWatchMetric extends AbstractAWSCredentialsProviderProcessor<AmazonCloudWatchClient> {

    public static final Set<Relationship> relationships = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    private static final Validator DOUBLE_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return (new ValidationResult.Builder()).subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
            } else {
                String reason = null;

                try {
                    Double.parseDouble(input);
                } catch (NumberFormatException e) {
                    reason = "not a valid Double";
                }

                return (new ValidationResult.Builder()).subject(subject).input(input).explanation(reason).valid(reason == null).build();
            }
        }
    };

    public static final PropertyDescriptor NAMESPACE = new PropertyDescriptor.Builder()
            .name("Namespace")
            .displayName("Namespace")
            .description("The namespace for the metric data for CloudWatch")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor METRIC_NAME = new PropertyDescriptor.Builder()
            .name("MetricName")
            .displayName("MetricName")
            .description("The name of the metric")
            .expressionLanguageSupported(true)
            .required(true)
            .addValidator(new StandardValidators.StringLengthValidator(1, 255))
            .build();

    public static final PropertyDescriptor VALUE = new PropertyDescriptor.Builder()
            .name("Value")
            .displayName("Value")
            .description("The value for the metric. Must be a double")
            .expressionLanguageSupported(true)
            .required(true)
            .addValidator(DOUBLE_VALIDATOR)
            .build();

    public static final PropertyDescriptor TIMESTAMP = new PropertyDescriptor.Builder()
            .name("Timestamp")
            .displayName("Timestamp")
            .description("A point in time expressed as the number of milliseconds since Jan 1, 1970 00:00:00 UTC. If not specified, the default value is set to the time the metric data was received")
            .expressionLanguageSupported(true)
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();

    public static final PropertyDescriptor UNIT = new PropertyDescriptor.Builder()
            .name("Unit")
            .displayName("Unit")
            .description("The unit of the metric. (e.g Seconds, Bytes, Megabytes, Percent, Count,  Kilobytes/Second, Terabits/Second, Count/Second) For details see http://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_MetricDatum.html")
            .expressionLanguageSupported(true)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final List<PropertyDescriptor> properties =
            Collections.unmodifiableList(
                    Arrays.asList(NAMESPACE, METRIC_NAME, VALUE, TIMESTAMP, UNIT, REGION, ACCESS_KEY, SECRET_KEY,
                            CREDENTIALS_FILE, AWS_CREDENTIALS_PROVIDER_SERVICE, TIMEOUT, SSL_CONTEXT_SERVICE,
                            ENDPOINT_OVERRIDE, PROXY_HOST, PROXY_HOST_PORT)
            );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    /**
     * Create client using aws credentials provider. This is the preferred way for creating clients
     */

    @Override
    protected AmazonCloudWatchClient createClient(ProcessContext processContext, AWSCredentialsProvider awsCredentialsProvider, ClientConfiguration clientConfiguration) {
        getLogger().info("Creating client using aws credentials provider");
        return new AmazonCloudWatchClient(awsCredentialsProvider, clientConfiguration);
    }

    /**
     * Create client using AWSCredentials
     *
     * @deprecated use {@link #createClient(ProcessContext, AWSCredentialsProvider, ClientConfiguration)} instead
     */
    @Override
    protected AmazonCloudWatchClient createClient(ProcessContext processContext, AWSCredentials awsCredentials, ClientConfiguration clientConfiguration) {
        getLogger().debug("Creating client with aws credentials");
        return new AmazonCloudWatchClient(awsCredentials, clientConfiguration);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        MetricDatum datum = new MetricDatum();

        try {
            datum.setMetricName(context.getProperty(METRIC_NAME).evaluateAttributeExpressions(flowFile).getValue());
            datum.setValue(Double.parseDouble(context.getProperty(VALUE).evaluateAttributeExpressions(flowFile).getValue()));

            final String timestamp = context.getProperty(TIMESTAMP).evaluateAttributeExpressions(flowFile).getValue();
            if (timestamp != null) {
                datum.setTimestamp(new Date(Long.parseLong(timestamp)));
            }

            final String unit = context.getProperty(UNIT).evaluateAttributeExpressions(flowFile).getValue();
            if (unit != null) {
                datum.setUnit(unit);
            }

            final PutMetricDataRequest metricDataRequest = new PutMetricDataRequest()
                    .withNamespace(context.getProperty(NAMESPACE).evaluateAttributeExpressions(flowFile).getValue())
                    .withMetricData(datum);

            putMetricData(metricDataRequest);
            session.transfer(flowFile, REL_SUCCESS);
            getLogger().info("Successfully published cloudwatch metric for {}", new Object[]{flowFile});
        } catch (final Exception e) {
            getLogger().error("Failed to publish cloudwatch metric for {} due to {}", new Object[]{flowFile, e});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }

    }

    protected PutMetricDataResult putMetricData(PutMetricDataRequest metricDataRequest) throws AmazonClientException {
        final AmazonCloudWatchClient client = getClient();
        final PutMetricDataResult result = client.putMetricData(metricDataRequest);
        return result;
    }

}