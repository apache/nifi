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

import static com.amazonaws.regions.Regions.US_EAST_1;
import static com.amazonaws.regions.Regions.US_WEST_2;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import org.apache.nifi.processor.ProcessContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TestAwsClientCache {

    @Mock
    private ProcessContext contextMock;
    @Mock
    private AwsClientProvider<AmazonWebServiceClient> awsClientProviderMock;
    @Mock
    private AmazonWebServiceClient awsClientMock1;
    @Mock
    private AmazonWebServiceClient awsClientMock2;
    private AwsClientCache<AmazonWebServiceClient> clientCache;

    @BeforeEach
    public void setup() {
        clientCache = new AwsClientCache<>();
    }

    @Test
    public void testSameRegionUseExistingClientFromCache() {
        final AwsClientDetails clientDetails = getClientDetails(US_WEST_2);
        when(awsClientProviderMock.createClient(contextMock, clientDetails)).thenReturn(awsClientMock1);
        final AmazonWebServiceClient client1 = clientCache.getOrCreateClient(contextMock, clientDetails, awsClientProviderMock);

        final AwsClientDetails newClientDetails = getClientDetails(US_WEST_2);
        final AmazonWebServiceClient client2 = clientCache.getOrCreateClient(contextMock, newClientDetails, awsClientProviderMock);
        verify(awsClientProviderMock, times(1)).createClient(eq(contextMock), any(AwsClientDetails.class));
        assertSame(client1, client2);
    }

    @Test
    public void testRegionChangeNewClientIsCreated() {
        final AwsClientDetails clientDetails = getClientDetails(US_WEST_2);
        when(awsClientProviderMock.createClient(contextMock, clientDetails)).thenReturn(awsClientMock1);
        final AmazonWebServiceClient client1 = clientCache.getOrCreateClient(contextMock, clientDetails, awsClientProviderMock);

        final AwsClientDetails newClientDetails = getClientDetails(US_EAST_1);
        when(awsClientProviderMock.createClient(contextMock, newClientDetails)).thenReturn(awsClientMock2);
        final AmazonWebServiceClient client2 = clientCache.getOrCreateClient(contextMock, newClientDetails, awsClientProviderMock);
        verify(awsClientProviderMock, times(2)).createClient(eq(contextMock), any(AwsClientDetails.class));
        assertNotEquals(client1, client2);
    }

    @Test
    public void testSameRegionClientCacheIsClearedNewClientIsCreated() {
        final AwsClientDetails clientDetails = getClientDetails(US_WEST_2);
        when(awsClientProviderMock.createClient(contextMock, clientDetails)).thenReturn(awsClientMock1);
        final AmazonWebServiceClient client1 = clientCache.getOrCreateClient(contextMock, clientDetails, awsClientProviderMock);

        clientCache.clearCache();

        final AwsClientDetails newClientDetails = getClientDetails(US_WEST_2);
        when(awsClientProviderMock.createClient(contextMock, clientDetails)).thenReturn(awsClientMock2);
        final AmazonWebServiceClient client2 = clientCache.getOrCreateClient(contextMock, newClientDetails, awsClientProviderMock);
        verify(awsClientProviderMock, times(2)).createClient(eq(contextMock), any(AwsClientDetails.class));
        assertNotEquals(client1, client2);
    }

    private static AwsClientDetails getClientDetails(Regions region) {
        return new AwsClientDetails(Region.getRegion(region));
    }
}
