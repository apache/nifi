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
package org.apache.nifi.web.standard.api.processor;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.Provider;

import org.apache.nifi.web.ComponentDescriptor;
import org.apache.nifi.web.ComponentDetails;
import org.apache.nifi.web.NiFiWebConfigurationContext;
import org.apache.nifi.web.NiFiWebConfigurationRequestContext;
import org.apache.nifi.web.NiFiWebRequestContext;


import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;
import org.mockito.Mockito;

import com.bazaarvoice.jolt.JsonUtils;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.core.ClassNamesResourceConfig;
import com.sun.jersey.spi.container.servlet.WebComponent;
import com.sun.jersey.spi.inject.SingletonTypeInjectableProvider;
import com.sun.jersey.test.framework.AppDescriptor;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;
import com.sun.jersey.test.framework.spi.container.TestContainerFactory;
import com.sun.jersey.test.framework.spi.container.inmemory.InMemoryTestContainerFactory;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;

public class TestProcessorResource extends JerseyTest {

    public static final ServletContext servletContext = mock(ServletContext.class);
    public static final HttpServletRequest requestContext = mock(HttpServletRequest.class);

    @Override
    protected AppDescriptor configure() {
        return new WebAppDescriptor.Builder()
                .initParam(WebComponent.RESOURCE_CONFIG_CLASS, ClassNamesResourceConfig.class.getName())
                .initParam(ClassNamesResourceConfig.PROPERTY_CLASSNAMES,
                        ProcessorResource.class.getName() +
                                ";" + TestProcessorResource.MockServletContext.class.getName() +
                                ";" + TestProcessorResource.MockRequestContext.class.getName()+";")
                .initParam("com.sun.jersey.api.json.POJOMappingFeature", "true")
                .build();
    }

    @Override
    public TestContainerFactory getTestContainerFactory() {
        return new InMemoryTestContainerFactory();
    }


    @Test
    public void testSetProperties() throws JSONException{

        final NiFiWebConfigurationContext niFiWebConfigurationContext = mock(NiFiWebConfigurationContext.class);
        final Map<String,String> properties = new HashMap<>();
        properties.put("jolt-transform","jolt-transform-chain");
        final ComponentDetails componentDetails = new ComponentDetails.Builder().properties(properties).build();

        Mockito.when(servletContext.getAttribute(Mockito.anyString())).thenReturn(niFiWebConfigurationContext);
        Mockito.when(niFiWebConfigurationContext.updateComponent(any(NiFiWebConfigurationRequestContext.class),any(String.class),any(Map.class))).thenReturn(componentDetails);

        ClientResponse response = client().resource(getBaseURI()).path("/standard/processor/properties")
                                                               .queryParam("processorId","1")
                                                               .queryParam("clientId","1")
                                                               .queryParam("revisionId","1")
                                                               .type(MediaType.APPLICATION_JSON_TYPE)
                                                               .put(ClientResponse.class,JsonUtils.toJsonString(properties));
        assertNotNull(response);
        JSONObject jsonObject = response.getEntity(JSONObject.class);
        assertNotNull(jsonObject);
        assertTrue(jsonObject.getJSONObject("properties").get("jolt-transform").equals("jolt-transform-chain"));
    }


    @Test
    public void testGetProcessorDetails() {
        final NiFiWebConfigurationContext niFiWebConfigurationContext = mock(NiFiWebConfigurationContext.class);
        final Map<String,String> allowableValues = new HashMap<>();
        final ComponentDescriptor descriptor = new ComponentDescriptor.Builder().name("test-name").allowableValues(allowableValues).build();
        final Map<String,ComponentDescriptor> descriptors = new HashMap<>();
        descriptors.put("jolt-transform",descriptor);
        final ComponentDetails componentDetails = new ComponentDetails.Builder().name("mytransform").type("org.apache.nifi.processors.standard.JoltTransformJSON")
                .descriptors(descriptors)
                .build();

        Mockito.when(servletContext.getAttribute(Mockito.anyString())).thenReturn(niFiWebConfigurationContext);
        Mockito.when(niFiWebConfigurationContext.getComponentDetails(any(NiFiWebRequestContext.class))).thenReturn(componentDetails);

        JSONObject value = client().resource(getBaseURI()).path("/standard/processor/details").queryParam("processorId","1").get(JSONObject.class);
        assertNotNull(value);

        try{
            assertTrue(value.get("name").equals("mytransform"));
        } catch (Exception e){
            fail("Failed due to: " + e.toString());
        }

    }

    @Provider
    public static class MockServletContext extends SingletonTypeInjectableProvider<Context, ServletContext> {

        public MockServletContext(){
            super(ServletContext.class, servletContext);
        }

    }

    @Provider
    public static class MockRequestContext extends SingletonTypeInjectableProvider<Context, HttpServletRequest> {

        public MockRequestContext(){
            super(HttpServletRequest.class, requestContext);
        }

    }


}
