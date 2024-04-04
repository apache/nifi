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
package org.apache.nifi.web.standard.api.transformjson;

import com.bazaarvoice.jolt.Diffy;
import com.bazaarvoice.jolt.JsonUtils;
import org.apache.nifi.web.ComponentDetails;
import org.apache.nifi.web.NiFiWebConfigurationContext;
import org.apache.nifi.web.NiFiWebRequestContext;
import org.apache.nifi.web.standard.api.transformjson.dto.JoltSpecificationDTO;
import org.apache.nifi.web.standard.api.transformjson.dto.ValidationDTO;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.inmemory.InMemoryTestContainerFactory;
import org.glassfish.jersey.test.spi.TestContainerFactory;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import jakarta.servlet.ServletContext;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;


public class TestTransformJSONResource extends JerseyTest {

    public static final ServletContext servletContext = mock(ServletContext.class);

    @Override
    protected Application configure() {
        final ResourceConfig config = new ResourceConfig();
        config.register(TransformJSONResource.class);
        config.register(JacksonFeature.class);
        config.register(new AbstractBinder(){
            @Override
            public void configure() {
                bindFactory(MockServletContext.class).to(ServletContext.class);
            }
        });
        return config;
    }

    @Override
    public TestContainerFactory getTestContainerFactory() {
        return new InMemoryTestContainerFactory();
    }


    @Test
    public void testValidateWithInvalidSpec() {

        final NiFiWebConfigurationContext niFiWebConfigurationContext = mock(NiFiWebConfigurationContext.class);
        final Map<String,String> properties = new HashMap<>();
        properties.put("jolt-transform","jolt-transform-chain");
        final ComponentDetails componentDetails = new ComponentDetails.Builder().properties(properties).build();
        Mockito.when(servletContext.getAttribute(Mockito.anyString())).thenReturn(niFiWebConfigurationContext);
        Mockito.when(niFiWebConfigurationContext.getComponentDetails(any(NiFiWebRequestContext.class))).thenReturn(componentDetails);

        JoltSpecificationDTO joltSpecificationDTO = new JoltSpecificationDTO("jolt-transform-chain","[]");
        ValidationDTO validate  = client().target(getBaseUri())
                .path("/standard/transformjson/validate")
                .request()
                .post(Entity.json(joltSpecificationDTO), ValidationDTO.class);

        assertNotNull(validate);
        assertFalse(validate.isValid());

    }

    @Test
    public void testValidateWithValidSpec() {
        JoltSpecificationDTO joltSpecificationDTO = new JoltSpecificationDTO("jolt-transform-remove","{\"rating\": {\"quality\": \"\"} }");
        ValidationDTO validation  = client().target(getBaseUri())
                .path("/standard/transformjson/validate")
                .request()
                .post(Entity.json(joltSpecificationDTO), ValidationDTO.class);

        assertNotNull(validation);
        assertTrue(validation.isValid());
    }

    @Test
    public void testValidateWithValidExpressionLanguageSpec() {
        JoltSpecificationDTO joltSpecificationDTO = new JoltSpecificationDTO("jolt-transform-remove","{\"rating\": {\"${filename}\": \"\"} }");
        ValidationDTO validation  = client().target(getBaseUri())
                .path("/standard/transformjson/validate")
                .request()
                .post(Entity.json(joltSpecificationDTO), ValidationDTO.class);

        assertNotNull(validation);
        assertTrue(validation.isValid());
    }

    @Test
    public void testValidateWithValidEmptySpec() {
        JoltSpecificationDTO joltSpecificationDTO = new JoltSpecificationDTO("jolt-transform-sort","");
        ValidationDTO validation  = client().target(getBaseUri())
                .path("/standard/transformjson/validate")
                .request()
                .post(Entity.json(joltSpecificationDTO), ValidationDTO.class);

        assertNotNull(validation);
        assertTrue(validation.isValid());
    }

    @Test
    public void testValidateWithInvalidEmptySpec() {
        JoltSpecificationDTO joltSpecificationDTO = new JoltSpecificationDTO("jolt-transform-remove","");
        ValidationDTO validation  = client().target(getBaseUri())
                .path("/standard/transformjson/validate")
                .request()
                .post(Entity.json(joltSpecificationDTO), ValidationDTO.class);

        assertNotNull(validation);
        assertFalse(validation.isValid());
    }

    @Test
    public void testValidateWithValidNullSpec() {
        JoltSpecificationDTO joltSpecificationDTO = new JoltSpecificationDTO("jolt-transform-sort",null);
        ValidationDTO validation  = client().target(getBaseUri())
                .path("/standard/transformjson/validate")
                .request()
                .post(Entity.json(joltSpecificationDTO), ValidationDTO.class);

        assertNotNull(validation);
        assertTrue(validation.isValid());
    }

    @Test
    public void testValidateWithCustomSpec() {

        final NiFiWebConfigurationContext niFiWebConfigurationContext = mock(NiFiWebConfigurationContext.class);
        final Map<String,String> properties = new HashMap<>();
        properties.put("jolt-transform","jolt-transform-custom");
        final ComponentDetails componentDetails = new ComponentDetails.Builder().properties(properties).build();
        Mockito.when(servletContext.getAttribute(Mockito.anyString())).thenReturn(niFiWebConfigurationContext);
        Mockito.when(niFiWebConfigurationContext.getComponentDetails(any(NiFiWebRequestContext.class))).thenReturn(componentDetails);

        JoltSpecificationDTO joltSpecificationDTO = new JoltSpecificationDTO("jolt-transform-custom","[{ \"operation\": \"default\", \"spec\":{ \"custom-id\" :4 }}]");
        joltSpecificationDTO.setCustomClass("TestCustomJoltTransform");
        ValidationDTO validate  = client().target(getBaseUri())
                .path("/standard/transformjson/validate")
                .request()
                .post(Entity.json(joltSpecificationDTO), ValidationDTO.class);

        assertNotNull(validate);
        assertFalse(validate.isValid());
    }

    @Test
    public void testValidateWithCustomSpecEmptyModule() {

        final NiFiWebConfigurationContext niFiWebConfigurationContext = mock(NiFiWebConfigurationContext.class);
        final Map<String,String> properties = new HashMap<>();
        properties.put("jolt-transform","jolt-transform-custom");
        final ComponentDetails componentDetails = new ComponentDetails.Builder().properties(properties).build();
        Mockito.when(servletContext.getAttribute(Mockito.anyString())).thenReturn(niFiWebConfigurationContext);
        Mockito.when(niFiWebConfigurationContext.getComponentDetails(any(NiFiWebRequestContext.class))).thenReturn(componentDetails);
        JoltSpecificationDTO joltSpecificationDTO = new JoltSpecificationDTO("jolt-transform-custom","[{ \"operation\": \"default\", \"spec\":{ \"custom-id\" :4 }}]");
        joltSpecificationDTO.setCustomClass("TestCustomJoltTransform");
        ValidationDTO validate  = client().target(getBaseUri())
                .path("/standard/transformjson/validate")
                .request()
                .post(Entity.json(joltSpecificationDTO), ValidationDTO.class);

        assertNotNull(validate);
        assertFalse(validate.isValid());
    }

    @Test
    public void testValidateWithCustomInvalidSpec() {

        final NiFiWebConfigurationContext niFiWebConfigurationContext = mock(NiFiWebConfigurationContext.class);
        final Map<String,String> properties = new HashMap<>();
        properties.put("jolt-transform","jolt-transform-custom");
        final ComponentDetails componentDetails = new ComponentDetails.Builder().properties(properties).build();
        Mockito.when(servletContext.getAttribute(Mockito.anyString())).thenReturn(niFiWebConfigurationContext);
        Mockito.when(niFiWebConfigurationContext.getComponentDetails(any(NiFiWebRequestContext.class))).thenReturn(componentDetails);

        JoltSpecificationDTO joltSpecificationDTO = new JoltSpecificationDTO("jolt-transform-custom","{ \"operation\": \"default\", \"spec\":{ \"custom-id\" :4 }}");
        joltSpecificationDTO.setCustomClass("TestCustomJoltTransform");
        ValidationDTO validate  = client().target(getBaseUri())
                .path("/standard/transformjson/validate")
                .request()
                .post(Entity.json(joltSpecificationDTO), ValidationDTO.class);

        assertNotNull(validate);
        assertFalse(validate.isValid());
    }

    @Test
    public void testExecuteWithValidCustomSpecEmptyModule() {
        JoltSpecificationDTO joltSpecificationDTO = new JoltSpecificationDTO("jolt-transform-custom","[{ \"operation\": \"default\", \"spec\":{ \"custom-id\" :4 }}]");
        String inputJson = "{\"rating\":{\"quality\":2,\"count\":1}}";
        joltSpecificationDTO.setInput(inputJson);
        joltSpecificationDTO.setCustomClass("TestCustomJoltTransform");
        final Response response = client().target(getBaseUri())
                .path("/standard/transformjson/execute")
                .request()
                .post(Entity.json(joltSpecificationDTO));

        assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    }

    @Test
    public void testExecuteWithInvalidSpec() {
        JoltSpecificationDTO joltSpecificationDTO = new JoltSpecificationDTO("jolt-transform-remove", "{\"rating\": {\"quality\": \"\"} }");
        final Response response = client().target(getBaseUri())
                .path("/standard/transformjson/execute")
                .request()
                .post(Entity.json(joltSpecificationDTO));

        assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    }

    @Test
    public void testExecuteWithValidSpec() {
        final Diffy diffy = new Diffy();
        JoltSpecificationDTO joltSpecificationDTO = new JoltSpecificationDTO("jolt-transform-remove","{\"rating\": {\"quality\": \"\"} }");
        String inputJson = "{\"rating\":{\"quality\":2,\"count\":1}}";
        joltSpecificationDTO.setInput(inputJson);
        String responseString = client().target(getBaseUri())
                .path("/standard/transformjson/execute")
                .request()
                .post(Entity.json(joltSpecificationDTO), String.class);

        Object transformedJson = JsonUtils.jsonToObject(responseString);
        Object compareJson = JsonUtils.jsonToObject("{\"rating\":{\"count\":1}}");
        assertNotNull(transformedJson);
        assertTrue(diffy.diff(compareJson, transformedJson).isEmpty());
    }

    @Test
    public void testExecuteWithValidExpressionLanguageSpec() {
        final Diffy diffy = new Diffy();
        JoltSpecificationDTO joltSpecificationDTO = new JoltSpecificationDTO("jolt-transform-shift","{ \"rating\" : {\"quality\": \"${qual_var}\"} }");
        String inputJson = "{\"rating\":{\"quality\":2,\"count\":1}}";
        joltSpecificationDTO.setInput(inputJson);
        Map<String,String> attributes = new HashMap<>();
        attributes.put("qual_var","qa");
        joltSpecificationDTO.setExpressionLanguageAttributes(attributes);
        String responseString = client().target(getBaseUri())
                .path("/standard/transformjson/execute")
                .request()
                .post(Entity.json(joltSpecificationDTO), String.class);

        Object transformedJson = JsonUtils.jsonToObject(responseString);
        Object compareJson = JsonUtils.jsonToObject( "{\"qa\":2}}");
        assertNotNull(transformedJson);
        assertTrue(diffy.diff(compareJson, transformedJson).isEmpty());
    }

    public static class MockServletContext implements Factory<ServletContext> {
        @Override
        public ServletContext provide() {
            return servletContext;
        }

        @Override
        public void dispose(ServletContext t) {
        }
    }

}
