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
 * See the License for the specific language governing permissions andf
 * limitations under the License.
 */
package org.apache.nifi.service.lookup;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockControllerServiceInitializationContext;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockValidationContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestAbstractSingleAttributeBasedControllerServiceLookup {
    private static final String LOOKUP_ATTRIBUTE = "lookupAttribute";
    private static final String TEST_SUBJECT_IDENTIFIER = "testSubjectIdentifier";
    private static final Class<ControllerService> SERVICE_TYPE = ControllerService.class;

    private final AbstractSingleAttributeBasedControllerServiceLookup<ControllerService> testSubject = Mockito.spy(AbstractSingleAttributeBasedControllerServiceLookup.class);

    private Map<PropertyDescriptor, String> properties;

    @Before
    public void setUp() throws Exception {
        when(testSubject.getLookupAttribute()).thenReturn(LOOKUP_ATTRIBUTE);
        when(testSubject.getServiceType()).thenReturn(SERVICE_TYPE);
        when(testSubject.getIdentifier()).thenReturn(TEST_SUBJECT_IDENTIFIER);

        properties = new HashMap<>();
    }

    @Test(expected = Exception.class)
    public void testLookupShouldThrowExceptionWhenQueriedServiceMappedInPropertiesButWasntCreated() {
        // GIVEN
        String mappedCreatedServiceID = "mappedCreatedServiceID";
        String mappedNotCreatedServiceID = "mappedNotCreatedServiceID";

        ControllerService mappedCreatedService = mock(SERVICE_TYPE);

        MockControllerServiceInitializationContext serviceLookup = new MockControllerServiceInitializationContext(mappedCreatedService, mappedCreatedServiceID);

        String dynamicProperty1 = "property1";
        String dynamicProperty2 = "property2";

        mapService(dynamicProperty1, mappedCreatedServiceID);
        mapService(dynamicProperty2, mappedNotCreatedServiceID);

        // WHEN
        testSubject.onEnabled(new MockConfigurationContext(properties, serviceLookup));
    }

    @Test
    public void testLookupShouldThrowExceptionWhenAttributeMapIsNull() {
        // GIVEN
        String mappedCreatedServiceID = "mappedCreatedServiceID";
        ControllerService mappedCreatedService = mock(SERVICE_TYPE);
        MockControllerServiceInitializationContext serviceLookup = new MockControllerServiceInitializationContext(mappedCreatedService, mappedCreatedServiceID);

        // WHEN
        testSubject.onEnabled(new MockConfigurationContext(properties, serviceLookup));
        try {
            testSubject.lookupService(null);
            fail();
        } catch (ProcessException e) {
            assertEquals("Attributes map is null", e.getMessage());
        }
    }

    @Test
    public void testLookupShouldThrowExceptionWhenAttributeMapHasNoLookupAttribute() {
        // GIVEN
        String mappedCreatedServiceID = "mappedCreatedServiceID";
        ControllerService mappedCreatedService = mock(SERVICE_TYPE);
        MockControllerServiceInitializationContext serviceLookup = new MockControllerServiceInitializationContext(mappedCreatedService, mappedCreatedServiceID);

        // WHEN
        testSubject.onEnabled(new MockConfigurationContext(properties, serviceLookup));
        try {
            testSubject.lookupService(new HashMap<>());
            fail();
        } catch (ProcessException e) {
            assertEquals("Attributes must contain an attribute name '" + LOOKUP_ATTRIBUTE + "'", e.getMessage());
        }
    }

    @Test
    public void testLookupShouldThrowExceptionWhenQueriedServiceWasCreatedButWasntMappedInProperties() {
        // GIVEN
        String mappedCreatedServiceID = "mappedCreatedServiceID";
        String notMappedCreatedServiceID = "notMappedCreatedServiceID";

        ControllerService mappedCreatedService = mock(SERVICE_TYPE);
        ControllerService notMappedCreatedService = mock(SERVICE_TYPE);

        MockControllerServiceInitializationContext serviceLookup = new MockControllerServiceInitializationContext(mappedCreatedService, mappedCreatedServiceID);
        serviceLookup.addControllerService(notMappedCreatedService, notMappedCreatedServiceID);

        String dynamicProperty1 = "property1";
        String dynamicProperty2 = "property2";

        mapService(dynamicProperty1, mappedCreatedServiceID);

        String lookupServiceKey = dynamicProperty2;

        // WHEN
        testSubject.onEnabled(new MockConfigurationContext(properties, serviceLookup));
        try {
            testSubject.lookupService(createAttributes(lookupServiceKey));
            fail();
        } catch (ProcessException e) {
            assertEquals("No ControllerService found for lookupAttribute", e.getMessage());
        }
    }

    @Test
    public void testLookupShouldReturnQueriedService() {
        // GIVEN
        String mappedCreatedServiceID1 = "mappedCreatedServiceID1";
        String mappedCreatedServiceID2 = "mappedCreatedServiceID2";

        ControllerService mappedCreatedService1 = mock(SERVICE_TYPE);
        ControllerService mappedCreatedService2 = mock(SERVICE_TYPE);

        MockControllerServiceInitializationContext serviceLookup = new MockControllerServiceInitializationContext(mappedCreatedService1, mappedCreatedServiceID1);
        serviceLookup.addControllerService(mappedCreatedService2, mappedCreatedServiceID2);

        String dynamicProperty1 = "property1";
        String dynamicProperty2 = "property2";

        mapService(dynamicProperty1, mappedCreatedServiceID1);
        mapService(dynamicProperty2, mappedCreatedServiceID2);

        String lookupServiceKey = dynamicProperty2;
        ControllerService expected = mappedCreatedService2;

        // WHEN
        testSubject.onEnabled(new MockConfigurationContext(properties, serviceLookup));
        ControllerService actual = testSubject.lookupService(createAttributes(lookupServiceKey));

        // THEN
        assertEquals(expected, actual);
    }

    @Test
    public void testCustomValidateShouldReturnErrorWhenNoServiceIsDefined() {
        // GIVEN
        ValidationContext context = new MockValidationContext(new MockProcessContext(testSubject));

        // WHEN
        Collection<ValidationResult> actual = testSubject.customValidate(context);

        // THEN
        assertThat(
                actual.stream().map(ValidationResult::getExplanation).collect(Collectors.toList()),
                hasItem(containsString("at least one " + SERVICE_TYPE.getSimpleName() + " must be defined via dynamic properties"))
        );
    }

    @Test
    public void testCustomValidateShouldReturnErrorWhenSelfAndOtherServiceIsMapped() {
        MockProcessContext processContext = new MockProcessContext(testSubject);
        processContext.setProperty("property1", "service1");
        processContext.setProperty("property2", TEST_SUBJECT_IDENTIFIER);

        ValidationContext context = new MockValidationContext(processContext);

        // WHEN
        Collection<ValidationResult> actual = testSubject.customValidate(context);

        // THEN
        assertThat(
                actual.stream().map(ValidationResult::getExplanation).collect(Collectors.toList()),
                hasItem(containsString("the current service cannot be registered as a " + SERVICE_TYPE.getSimpleName() + " to lookup"))
        );
    }

    @Test
    public void testCustomValidateShouldReturnErrorsWhenOnlySelfIsMapped() {
        MockProcessContext processContext = new MockProcessContext(testSubject);
        processContext.setProperty("property1", TEST_SUBJECT_IDENTIFIER);

        ValidationContext context = new MockValidationContext(processContext);

        // WHEN
        Collection<ValidationResult> actual = testSubject.customValidate(context);

        // THEN
        assertThat(
                actual.stream().map(ValidationResult::getExplanation).collect(Collectors.toList()),
                hasItems(
                        containsString("the current service cannot be registered as a " + SERVICE_TYPE.getSimpleName() + " to lookup"),
                        containsString("at least one " + SERVICE_TYPE.getSimpleName() + " must be defined via dynamic properties")
                )
        );
    }

    @Test
    public void testCustomValidateShouldReturnNoErrorWhenAServiceIsDefined() {
        MockProcessContext processContext = new MockProcessContext(testSubject);
        processContext.setProperty("property1", "service1");

        ValidationContext context = new MockValidationContext(processContext);

        // WHEN
        Collection<ValidationResult> actual = testSubject.customValidate(context);

        // THEN
        assertEquals(Collections.emptyList(), new ArrayList<>(actual));
    }

    @Test
    public void testGetServiceType() {
        Class<ControllerService> actual = testSubject.getServiceType();
        assertEquals(SERVICE_TYPE, actual);
    }

    @Test
    public void testLookupAttribute() {
        String actual = testSubject.getLookupAttribute();
        assertEquals(LOOKUP_ATTRIBUTE, actual);
    }

    private void mapService(String dynamicProperty, String registeredService) {
        properties.put(
                new PropertyDescriptor.Builder()
                        .name(dynamicProperty)
                        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                        .dynamic(true)
                        .build(),
                registeredService
        );
    }

    private Map<String, String> createAttributes(final String lookupValue) {
        Map<String, String> attributes = new HashMap<String, String>() {{
            put(LOOKUP_ATTRIBUTE, lookupValue);
        }};

        return attributes;
    }
}
