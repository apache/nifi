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
package org.apache.nifi.processors.aws;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;

/**
 * Base class for aws processors that uses AWSCredentialsProvider interface for creating aws clients.
 *
 * @param <ClientType> client type
 *
 * @see <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html">AWSCredentialsProvider</a>
 */
public abstract class AbstractAWSCredentialsProviderProcessor<ClientType extends AmazonWebServiceClient>
    extends AbstractAWSProcessor<ClientType>  {

    /**
     * AWS credentials provider service
     *
     * @see  <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html">AWSCredentialsProvider</a>
     */
    public static final PropertyDescriptor AWS_CREDENTIALS_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .name("AWS Credentials Provider service")
            .description("The Controller Service that is used to obtain aws credentials provider")
            .required(false)
            .identifiesControllerService(AWSCredentialsProviderService.class)
            .build();

    /**
     * This method checks if {#link {@link #AWS_CREDENTIALS_PROVIDER_SERVICE} is available and if it
     * is, uses the credentials provider, otherwise it invokes the {@link AbstractAWSProcessor#onScheduled(ProcessContext)}
     * which uses static AWSCredentials for the aws processors
     */
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        ControllerService service = context.getProperty(AWS_CREDENTIALS_PROVIDER_SERVICE).asControllerService();
        if (service != null) {
            getLogger().debug("Using aws credentials provider service for creating client");
            onScheduledUsingControllerService(context);
        } else {
            getLogger().debug("Using aws credentials for creating client");
            super.onScheduled(context);
        }
    }

    /**
     * Create aws client using credentials provider
     * @param context the process context
     */
    protected void onScheduledUsingControllerService(ProcessContext context) {
        this.client = createClient(context, getCredentialsProvider(context), createConfiguration(context));
        super.initializeRegionAndEndpoint(context);

     }

    @OnShutdown
    public void onShutDown() {
        if ( this.client != null ) {
           this.client.shutdown();
        }
    }

    /**
     * Get credentials provider using the {@link AWSCredentialsProviderService}
     * @param context the process context
     * @return AWSCredentialsProvider the credential provider
     * @see  <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html">AWSCredentialsProvider</a>
     */
    protected AWSCredentialsProvider getCredentialsProvider(final ProcessContext context) {

        final AWSCredentialsProviderService awsCredentialsProviderService =
              context.getProperty(AWS_CREDENTIALS_PROVIDER_SERVICE).asControllerService(AWSCredentialsProviderService.class);

        return awsCredentialsProviderService.getCredentialsProvider();

    }

    /**
     * Abstract method to create aws client using credentials provider.  This is the preferred method
     * for creating aws clients
     * @param context process context
     * @param credentialsProvider aws credentials provider
     * @param config aws client configuration
     * @return ClientType the client
     */
    protected abstract ClientType createClient(final ProcessContext context, final AWSCredentialsProvider credentialsProvider, final ClientConfiguration config);
}