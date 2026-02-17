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
package org.apache.nifi.processors.aws.s3;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AbstractS3ProcessorTest {

    private static final StaticCredentialsProvider TEST_CREDENTIALS =
            StaticCredentialsProvider.create(AwsBasicCredentials.create("test-access-key", "test-secret-key"));

    /**
     * Verifies that the S3 client builder returned by {@link AbstractS3Processor#createClientBuilder}
     * resolves the us-east-1 endpoint to the regional endpoint (s3.us-east-1.amazonaws.com)
     * rather than the global endpoint (s3.amazonaws.com).
     *
     * <p>AWS SDK v2 has a bug where DefaultsMode.STANDARD does not properly configure regional
     * endpoints for S3 in us-east-1. Without a workaround in createClientBuilder, the SDK
     * resolves to the global endpoint and this test fails.
     */
    @Test
    void testUsEast1EndpointIsRegional() {
        final ProcessContext mockContext = createMockProcessContext();
        final TestableS3Processor processor = new TestableS3Processor();

        final S3ClientBuilderWrapper clientBuilder = processor.callCreateClientBuilder(mockContext);

        final AtomicReference<String> capturedHost = new AtomicReference<>();
        final ExecutionInterceptor hostCapturingInterceptor = new ExecutionInterceptor() {
            @Override
            public SdkHttpRequest modifyHttpRequest(final Context.ModifyHttpRequest context, final ExecutionAttributes executionAttributes) {
                capturedHost.set(context.httpRequest().host());
                throw new RuntimeException("Endpoint captured");
            }
        };

        clientBuilder.overrideConfiguration(c -> c.addExecutionInterceptor(hostCapturingInterceptor));
        clientBuilder.region(Region.US_EAST_1);
        clientBuilder.credentialsProvider(TEST_CREDENTIALS);

        try (final S3Client client = clientBuilder.build()) {
            client.listBuckets();
        } catch (final Exception expected) {
            assertNotNull(expected.getMessage());
        }

        assertNotNull(capturedHost.get(), "Interceptor should have captured the endpoint host");
        assertTrue(capturedHost.get().contains("s3.us-east-1.amazonaws.com"),
                "S3 client should use regional endpoint for us-east-1 but resolved to: " + capturedHost.get());
    }

    private static ProcessContext createMockProcessContext() {
        final ProcessContext context = mock(ProcessContext.class);

        final PropertyValue nullPropertyValue = mock(PropertyValue.class);
        when(nullPropertyValue.asControllerService(any())).thenReturn(null);
        when(nullPropertyValue.asBoolean()).thenReturn(null);
        when(nullPropertyValue.evaluateAttributeExpressions()).thenReturn(nullPropertyValue);
        when(nullPropertyValue.getValue()).thenReturn(null);

        when(context.getProperty(any(PropertyDescriptor.class))).thenReturn(nullPropertyValue);

        return context;
    }

    /**
     * Minimal concrete subclass that exposes createClientBuilder for testing.
     */
    private static class TestableS3Processor extends AbstractS3Processor {

        S3ClientBuilderWrapper callCreateClientBuilder(final ProcessContext context) {
            return createClientBuilder(context);
        }

        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        }

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return List.of();
        }
    }
}
