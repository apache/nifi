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

import junit.framework.TestCase;

import java.util.HashMap;
import java.util.Map;


import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;


import org.apache.nifi.web.ComponentDetails;
import org.apache.nifi.web.NiFiWebConfigurationContext;
import org.apache.nifi.web.NiFiWebRequestContext;
import org.apache.nifi.web.standard.api.transformjson.dto.JoltSpecificationDTO;
import org.apache.nifi.web.standard.api.transformjson.dto.ValidationDTO;


import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;


import com.bazaarvoice.jolt.Diffy;
import com.bazaarvoice.jolt.JsonUtils;
import com.sun.jersey.api.client.UniformInterfaceException;
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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;


public class TestTransformJSONResource extends JerseyTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    public static final ServletContext servletContext = mock(ServletContext.class);
    public static final HttpServletRequest requestContext = mock(HttpServletRequest.class);

    @Override
    protected AppDescriptor configure() {
        return new WebAppDescriptor.Builder()
                .initParam(WebComponent.RESOURCE_CONFIG_CLASS, ClassNamesResourceConfig.class.getName())
                .initParam(ClassNamesResourceConfig.PROPERTY_CLASSNAMES,
                        TransformJSONResource.class.getName() + ";" + MockServletContext.class.getName() + ";" + MockRequestContext.class.getName()+";")
                .initParam("com.sun.jersey.api.json.POJOMappingFeature", "true")
                .build();
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
        ValidationDTO validate  = client().resource(getBaseURI()).path("/standard/transformjson/validate").post(ValidationDTO.class, joltSpecificationDTO);

        assertNotNull(validate);
        assertTrue(!validate.isValid());

    }

    @Test
    public void testValidateWithValidSpec() {
        JoltSpecificationDTO joltSpecificationDTO = new JoltSpecificationDTO("jolt-transform-remove","{\"rating\": {\"quality\": \"\"} }");
        ValidationDTO validation  = client().resource(getBaseURI()).path("/standard/transformjson/validate").post(ValidationDTO.class, joltSpecificationDTO);
        TestCase.assertNotNull(validation);
        assertTrue(validation.isValid());
    }

    @Test
    public void testValidateWithValidEmptySpec() {
        JoltSpecificationDTO joltSpecificationDTO = new JoltSpecificationDTO("jolt-transform-sort","");
        ValidationDTO validation  = client().resource(getBaseURI()).path("/standard/transformjson/validate").post(ValidationDTO.class, joltSpecificationDTO);
        TestCase.assertNotNull(validation);
        assertTrue(validation.isValid());
    }

    @Test
    public void testValidateWithInvalidEmptySpec() {
        JoltSpecificationDTO joltSpecificationDTO = new JoltSpecificationDTO("jolt-transform-remove","");
        ValidationDTO validation  = client().resource(getBaseURI()).path("/standard/transformjson/validate").post(ValidationDTO.class, joltSpecificationDTO);
        TestCase.assertNotNull(validation);
        assertTrue(!validation.isValid());
    }

    @Test
    public void testValidateWithValidNullSpec() {
        JoltSpecificationDTO joltSpecificationDTO = new JoltSpecificationDTO("jolt-transform-sort",null);
        ValidationDTO validation  = client().resource(getBaseURI()).path("/standard/transformjson/validate").post(ValidationDTO.class, joltSpecificationDTO);
        TestCase.assertNotNull(validation);
        assertTrue(validation.isValid());
    }


    @Test
    public void testExecuteWithInvalidSpec() {
        JoltSpecificationDTO joltSpecificationDTO = new JoltSpecificationDTO("jolt-transform-remove", "{\"rating\": {\"quality\": \"\"} }");
        exception.expect(UniformInterfaceException.class);
        client().resource(getBaseURI()).path("/standard/transformjson/execute").post(joltSpecificationDTO);
    }

    @Test
    public void testExecuteWithValidSpec() {
        final Diffy diffy = new Diffy();
        JoltSpecificationDTO joltSpecificationDTO = new JoltSpecificationDTO("jolt-transform-remove","{\"rating\": {\"quality\": \"\"} }");
        String inputJson = "{\"rating\":{\"quality\":2,\"count\":1}}";
        joltSpecificationDTO.setInput(inputJson);
        String responseString = client().resource(getBaseURI()).path("/standard/transformjson/execute").post(String.class, joltSpecificationDTO);
        Object transformedJson = JsonUtils.jsonToObject(responseString);
        Object compareJson = JsonUtils.jsonToObject("{\"rating\":{\"count\":1}}");
        assertNotNull(transformedJson);
        assertTrue(diffy.diff(compareJson, transformedJson).isEmpty());
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
