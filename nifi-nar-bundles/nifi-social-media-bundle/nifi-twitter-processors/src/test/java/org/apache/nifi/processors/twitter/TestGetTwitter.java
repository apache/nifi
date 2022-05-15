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
package org.apache.nifi.processors.twitter;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import java.util.concurrent.BlockingQueue;


@ExtendWith(MockitoExtension.class)
public class TestGetTwitter {
    private TestRunner runner;
    @Mock
    Client client = Mockito.mock(Client.class);
    @Mock
    BlockingQueue<String> messageQueue = Mockito.mock(BlockingQueue.class);
    @InjectMocks
    GetTwitter getTwitter = new GetTwitter();

    @BeforeEach
    public void init() {
        runner = TestRunners.newTestRunner(getTwitter);
    }


    @Test
    public void testLocationValidatorWithValidLocations() {
        runner.setProperty(GetTwitter.ENDPOINT, GetTwitter.ENDPOINT_FILTER);
        runner.setProperty(GetTwitter.MAX_CLIENT_ERROR_RETRIES, "5");
        runner.setProperty(GetTwitter.CONSUMER_KEY, "consumerKey");
        runner.setProperty(GetTwitter.CONSUMER_SECRET, "consumerSecret");
        runner.setProperty(GetTwitter.ACCESS_TOKEN, "accessToken");
        runner.setProperty(GetTwitter.ACCESS_TOKEN_SECRET, "accessTokenSecret");
        runner.setProperty(GetTwitter.LOCATIONS, "-122.75,36.8,-121.75,37.8,-74,40,-73,41");
        runner.assertValid();
    }

    @Test
    public void testLocationValidatorWithEqualLatitudes() {
        runner.setProperty(GetTwitter.ENDPOINT, GetTwitter.ENDPOINT_FILTER);
        runner.setProperty(GetTwitter.MAX_CLIENT_ERROR_RETRIES, "5");
        runner.setProperty(GetTwitter.CONSUMER_KEY, "consumerKey");
        runner.setProperty(GetTwitter.CONSUMER_SECRET, "consumerSecret");
        runner.setProperty(GetTwitter.ACCESS_TOKEN, "accessToken");
        runner.setProperty(GetTwitter.ACCESS_TOKEN_SECRET, "accessTokenSecret");
        runner.setProperty(GetTwitter.LOCATIONS, "-122.75,36.8,-121.75,37.8,-74,40,-73,40");
        runner.assertNotValid();
    }

    @Test
    public void testLocationValidatorWithEqualLongitudes() {
        runner.setProperty(GetTwitter.ENDPOINT, GetTwitter.ENDPOINT_FILTER);
        runner.setProperty(GetTwitter.MAX_CLIENT_ERROR_RETRIES, "5");
        runner.setProperty(GetTwitter.CONSUMER_KEY, "consumerKey");
        runner.setProperty(GetTwitter.CONSUMER_SECRET, "consumerSecret");
        runner.setProperty(GetTwitter.ACCESS_TOKEN, "accessToken");
        runner.setProperty(GetTwitter.ACCESS_TOKEN_SECRET, "accessTokenSecret");
        runner.setProperty(GetTwitter.LOCATIONS, "-122.75,36.8,-121.75,37.8,-74,40,-74,41");
        runner.assertNotValid();
    }

    @Test
    public void testLocationValidatorWithSWLatGreaterThanNELat() {
        runner.setProperty(GetTwitter.ENDPOINT, GetTwitter.ENDPOINT_FILTER);
        runner.setProperty(GetTwitter.MAX_CLIENT_ERROR_RETRIES, "5");
        runner.setProperty(GetTwitter.CONSUMER_KEY, "consumerKey");
        runner.setProperty(GetTwitter.CONSUMER_SECRET, "consumerSecret");
        runner.setProperty(GetTwitter.ACCESS_TOKEN, "accessToken");
        runner.setProperty(GetTwitter.ACCESS_TOKEN_SECRET, "accessTokenSecret");
        runner.setProperty(GetTwitter.LOCATIONS, "-122.75,36.8,-121.75,37.8,-74,40,-73,39");
        runner.assertNotValid();
    }

    @Test
    public void testLocationValidatorWithSWLonGreaterThanNELon() {
        runner.setProperty(GetTwitter.ENDPOINT, GetTwitter.ENDPOINT_FILTER);
        runner.setProperty(GetTwitter.MAX_CLIENT_ERROR_RETRIES, "5");
        runner.setProperty(GetTwitter.CONSUMER_KEY, "consumerKey");
        runner.setProperty(GetTwitter.CONSUMER_SECRET, "consumerSecret");
        runner.setProperty(GetTwitter.ACCESS_TOKEN, "accessToken");
        runner.setProperty(GetTwitter.ACCESS_TOKEN_SECRET, "accessTokenSecret");
        runner.setProperty(GetTwitter.LOCATIONS, "-122.75,36.8,-121.75,37.8,-74,40,-75,41");
        runner.assertNotValid();
    }


    // To test getSupportedDynamicPropertyDescriptor
    @Test
    public void testValidGetSupportedDynamicPropertyDescriptor() {
        runner.setProperty(GetTwitter.MAX_CLIENT_ERROR_RETRIES, "5");
        runner.setProperty(GetTwitter.CONSUMER_KEY, "consumerKey");
        runner.setProperty(GetTwitter.CONSUMER_SECRET, "consumerSecret");
        runner.setProperty(GetTwitter.ACCESS_TOKEN, "accessToken");
        runner.setProperty(GetTwitter.ACCESS_TOKEN_SECRET, "accessTokenSecret");
        PropertyDescriptor dynamicProperty = new PropertyDescriptor.Builder()
                .name("foo")
                .description("Adds a query parameter with name '" + "foo" + "' to the Twitter query")
                .required(false)
                .dynamic(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();
        runner.setProperty(dynamicProperty, "{\"a\": \"a\"}");
        runner.assertValid();
    }


    // To test customValidate - lines 222 to 224
    @Test
    public void testCustomValidatorWithoutTermsFollowingLocation() {
        runner.setProperty(GetTwitter.ENDPOINT, GetTwitter.ENDPOINT_FILTER);
        runner.setProperty(GetTwitter.MAX_CLIENT_ERROR_RETRIES, "5");
        runner.setProperty(GetTwitter.CONSUMER_KEY, "consumerKey");
        runner.setProperty(GetTwitter.CONSUMER_SECRET, "consumerSecret");
        runner.setProperty(GetTwitter.ACCESS_TOKEN, "accessToken");
        runner.setProperty(GetTwitter.ACCESS_TOKEN_SECRET, "accessTokenSecret");
        runner.assertNotValid();
    }

    // To test onScheduled using ENDPOINT_SAMPLE and language
    // Mocking Client and messageQueue instead to avoid make calls to the Twitter service
    @Test
    public void testRunsOnSchedulerEndpointSampleAndLanguage() {
        Mockito.when(messageQueue.poll()).thenReturn("Hello World!");
        StreamingEndpoint streamep = Mockito.mock(StreamingEndpoint.class);
        Mockito.when(client.getEndpoint()).thenReturn(streamep);
        runner.setProperty(GetTwitter.ENDPOINT, GetTwitter.ENDPOINT_SAMPLE);
        runner.setProperty(GetTwitter.MAX_CLIENT_ERROR_RETRIES, "5");
        runner.setProperty(GetTwitter.CONSUMER_KEY, "consumerKey");
        runner.setProperty(GetTwitter.CONSUMER_SECRET, "consumerSecret");
        runner.setProperty(GetTwitter.ACCESS_TOKEN, "accessToken");
        runner.setProperty(GetTwitter.ACCESS_TOKEN_SECRET, "accessTokenSecret");
        runner.setProperty(GetTwitter.LANGUAGES, "en, pt, it");
        runner.assertValid();
        runner.run();
    }

    // To test onScheduled using ENDPOINT_SAMPLE
    // Mocking Client and messageQueue instead to avoid make calls to the Twitter service
    @Test
    public void testRunsOnSchedulerEndpointSample() {
        Mockito.when(messageQueue.poll()).thenReturn("Hello World!");
        StreamingEndpoint streamep = Mockito.mock(StreamingEndpoint.class);
        Mockito.when(client.getEndpoint()).thenReturn(streamep);
        runner.setProperty(GetTwitter.ENDPOINT, GetTwitter.ENDPOINT_SAMPLE);
        runner.setProperty(GetTwitter.MAX_CLIENT_ERROR_RETRIES, "5");
        runner.setProperty(GetTwitter.CONSUMER_KEY, "consumerKey");
        runner.setProperty(GetTwitter.CONSUMER_SECRET, "consumerSecret");
        runner.setProperty(GetTwitter.ACCESS_TOKEN, "accessToken");
        runner.setProperty(GetTwitter.ACCESS_TOKEN_SECRET, "accessTokenSecret");
        runner.assertValid();
        runner.run(1);
    }


    // To test onScheduled using ENDPOINT_FILTER with valid locations, and language list
    // Mocking Client and messageQueue instead to avoid make calls to the Twitter service
    @Test
    public void testRunsOnSchedulerEndpointFilterAndLanguage() {
        Mockito.when(messageQueue.poll()).thenReturn("Hello World!");
        StreamingEndpoint streamep = Mockito.mock(StreamingEndpoint.class);
        Mockito.when(client.getEndpoint()).thenReturn(streamep);
        runner.setProperty(GetTwitter.ENDPOINT, GetTwitter.ENDPOINT_FILTER);
        runner.setProperty(GetTwitter.MAX_CLIENT_ERROR_RETRIES, "5");
        runner.setProperty(GetTwitter.CONSUMER_KEY, "consumerKey");
        runner.setProperty(GetTwitter.CONSUMER_SECRET, "consumerSecret");
        runner.setProperty(GetTwitter.ACCESS_TOKEN, "accessToken");
        runner.setProperty(GetTwitter.ACCESS_TOKEN_SECRET, "accessTokenSecret");
        runner.setProperty(GetTwitter.LOCATIONS, "-122.75,36.8,-121.75,37.8,-74,40,-73,41");
        runner.setProperty(GetTwitter.LANGUAGES, "en, pt, it");
        runner.assertValid();
        runner.run(1);
    }

    // To test onScheduled using ENDPOINT_FILTER with valid TERMS and no language, and no location
    // Mocking Client and messageQueue instead to avoid make calls to the Twitter service
    @Test
    public void testRunsOnSchedulerEndpointFilterAndTerms() {
        Mockito.when(messageQueue.poll()).thenReturn("Hello World!");
        StreamingEndpoint streamep = Mockito.mock(StreamingEndpoint.class);
        Mockito.when(client.getEndpoint()).thenReturn(streamep);
        runner.setProperty(GetTwitter.ENDPOINT, GetTwitter.ENDPOINT_FILTER);
        runner.setProperty(GetTwitter.MAX_CLIENT_ERROR_RETRIES, "5");
        runner.setProperty(GetTwitter.CONSUMER_KEY, "consumerKey");
        runner.setProperty(GetTwitter.CONSUMER_SECRET, "consumerSecret");
        runner.setProperty(GetTwitter.ACCESS_TOKEN, "accessToken");
        runner.setProperty(GetTwitter.ACCESS_TOKEN_SECRET, "accessTokenSecret");
        runner.setProperty(GetTwitter.TERMS, "any thing we want to filter");
        runner.assertValid();
        runner.run(1);
    }

    // To test onScheduled using ENDPOINT_FILTER with IDs to follow
    // Mocking Client and messageQueue instead to avoid make calls to the Twitter service
    @Test
    public void testRunsOnSchedulerEndpointFilterAndID() {
        Mockito.when(messageQueue.poll()).thenReturn("Hello World!");
        StreamingEndpoint streamep = Mockito.mock(StreamingEndpoint.class);
        Mockito.when(client.getEndpoint()).thenReturn(streamep);
        runner.setProperty(GetTwitter.ENDPOINT, GetTwitter.ENDPOINT_FILTER);
        runner.setProperty(GetTwitter.MAX_CLIENT_ERROR_RETRIES, "5");
        runner.setProperty(GetTwitter.CONSUMER_KEY, "consumerKey");
        runner.setProperty(GetTwitter.CONSUMER_SECRET, "consumerSecret");
        runner.setProperty(GetTwitter.ACCESS_TOKEN, "accessToken");
        runner.setProperty(GetTwitter.ACCESS_TOKEN_SECRET, "accessTokenSecret");
        String followingIds = "   4265731,\n" +
                "    27674040,\n" +
                "    26123649,\n" +
                "    9576402,\n" +
                "    821958,\n" +
                "    7852612,\n" +
                "    819797\n";
        runner.setProperty(GetTwitter.FOLLOWING, followingIds);
        runner.assertValid();
        runner.run(1);
    }

    // To test onScheduled using ENDPOINT_FIREHOUSE and languages list
    // Mocking Client and messageQueue instead to avoid make calls to the Twitter service
    @Test
    public void testRunsOnSchedulerEndpointFirehouseAndLanguage() {
        Mockito.when(messageQueue.poll()).thenReturn("Hello World!");
        StreamingEndpoint streamep = Mockito.mock(StreamingEndpoint.class);
        Mockito.when(client.getEndpoint()).thenReturn(streamep);
        runner.setProperty(GetTwitter.ENDPOINT, GetTwitter.ENDPOINT_FIREHOSE);
        runner.setProperty(GetTwitter.MAX_CLIENT_ERROR_RETRIES, "5");
        runner.setProperty(GetTwitter.CONSUMER_KEY, "consumerKey");
        runner.setProperty(GetTwitter.CONSUMER_SECRET, "consumerSecret");
        runner.setProperty(GetTwitter.ACCESS_TOKEN, "accessToken");
        runner.setProperty(GetTwitter.ACCESS_TOKEN_SECRET, "accessTokenSecret");
        runner.setProperty(GetTwitter.LANGUAGES, "en, pt, it");
        runner.assertValid();
        runner.run(1);
    }

    // To test FollowingValidator for Invalid Following - not number
    // and test catch invalid location values
    @Test
    public void testCustomValidatorInvalidFollowingLocation() {
        runner.setProperty(GetTwitter.ENDPOINT, GetTwitter.ENDPOINT_FILTER);
        runner.setProperty(GetTwitter.MAX_CLIENT_ERROR_RETRIES, "5");
        runner.setProperty(GetTwitter.CONSUMER_KEY, "consumerKey");
        runner.setProperty(GetTwitter.CONSUMER_SECRET, "consumerSecret");
        runner.setProperty(GetTwitter.ACCESS_TOKEN, "accessToken");
        runner.setProperty(GetTwitter.ACCESS_TOKEN_SECRET, "accessTokenSecret");
        runner.setProperty(GetTwitter.FOLLOWING, "invalid id value");
        runner.setProperty(GetTwitter.LOCATIONS, "invalid location value");
        runner.assertNotValid();
    }
}