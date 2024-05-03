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
package org.apache.nifi.documentation.html;

import org.apache.nifi.documentation.DocumentationWriter;
import org.apache.nifi.python.PythonProcessorDetails;
import org.apache.nifi.python.processor.documentation.MultiProcessorUseCaseDetails;
import org.apache.nifi.python.processor.documentation.ProcessorConfigurationDetails;
import org.apache.nifi.python.processor.documentation.PropertyDescription;
import org.apache.nifi.python.processor.documentation.UseCaseDetails;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import static org.apache.nifi.documentation.html.AbstractHtmlDocumentationWriter.NO_DESCRIPTION;
import static org.apache.nifi.documentation.html.AbstractHtmlDocumentationWriter.NO_PROPERTIES;
import static org.apache.nifi.documentation.html.AbstractHtmlDocumentationWriter.NO_TAGS;
import static org.apache.nifi.documentation.html.XmlValidator.assertContains;
import static org.apache.nifi.documentation.html.XmlValidator.assertNotContains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HtmlPythonProcessorDocumentationWriterTest {

    @Test
    public void testProcessorDocumentation() throws IOException {
        final PythonProcessorDetails processorDetails = getPythonProcessorDetails();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        final DocumentationWriter<PythonProcessorDetails> writer = new HtmlPythonProcessorDocumentationWriter();
        writer.write(processorDetails, outputStream, false);

        final String results = outputStream.toString();
        XmlValidator.assertXmlValid(results);

        assertContains(results, "This is a test capability description");
        assertContains(results, "tag1, tag2, tag3");

        final List<PropertyDescription> propertyDescriptions = getPropertyDescriptions();
        propertyDescriptions.forEach(propertyDescription -> {
            assertContains(results, propertyDescription.getDisplayName());
            assertContains(results, propertyDescription.getDescription());
            assertContains(results, propertyDescription.getDefaultValue());
        });

        assertContains(results, "Supports Expression Language: true (will be evaluated using Environment variables only)");
        assertContains(results, "Supports Expression Language: true (will be evaluated using flow file attributes and Environment variables)");

        final List<UseCaseDetails> useCases = getUseCases();
        useCases.forEach(useCase -> {
            assertContains(results, useCase.getDescription());
            assertContains(results, useCase.getNotes());
            assertContains(results, String.join(", ", useCase.getKeywords()));
            assertContains(results, useCase.getConfiguration());
        });

        final List<MultiProcessorUseCaseDetails> multiProcessorUseCases = getMultiProcessorUseCases();
        multiProcessorUseCases.forEach(multiProcessorUseCase -> {
            assertContains(results, multiProcessorUseCase.getDescription());
            assertContains(results, multiProcessorUseCase.getNotes());
            assertContains(results, String.join(", ", multiProcessorUseCase.getKeywords()));

            multiProcessorUseCase.getConfigurations().forEach(configuration -> {
                assertContains(results, configuration.getProcessorType());
                assertContains(results, configuration.getConfiguration());
            });
        });

        assertNotContains(results, NO_PROPERTIES);
        assertNotContains(results, "No description provided.");
        assertNotContains(results, NO_TAGS);
    }

    @Test
    public void testEmptyProcessor() throws IOException {
        final PythonProcessorDetails processorDetails = mock(PythonProcessorDetails.class);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        final DocumentationWriter<PythonProcessorDetails> writer = new HtmlPythonProcessorDocumentationWriter();
        writer.write(processorDetails, outputStream, false);

        final String results = outputStream.toString();
        XmlValidator.assertXmlValid(results);

        assertContains(results, NO_DESCRIPTION);
        assertContains(results, NO_TAGS);
        assertContains(results, NO_PROPERTIES);
    }

    private PythonProcessorDetails getPythonProcessorDetails() {
        final PythonProcessorDetails processorDetails = mock(PythonProcessorDetails.class);
        when(processorDetails.getProcessorType()).thenReturn("TestPythonProcessor");
        when(processorDetails.getProcessorVersion()).thenReturn("1.0.0");
        when(processorDetails.getSourceLocation()).thenReturn("/source/location/TestPythonProcessor.py");
        when(processorDetails.getCapabilityDescription()).thenReturn("This is a test capability description");
        when(processorDetails.getTags()).thenReturn(List.of("tag1", "tag2", "tag3"));
        when(processorDetails.getDependencies()).thenReturn(List.of("dependency1==0.1", "dependency2==0.2"));
        when(processorDetails.getInterface()).thenReturn("org.apache.nifi.python.processor.FlowFileTransform");
        when(processorDetails.getUseCases()).thenAnswer(invocation -> getUseCases());
        when(processorDetails.getMultiProcessorUseCases()).thenAnswer(invocation -> getMultiProcessorUseCases());
        when(processorDetails.getPropertyDescriptions()).thenAnswer(invocation -> getPropertyDescriptions());

        return processorDetails;
    }

    private List<UseCaseDetails> getUseCases() {
        final UseCaseDetails useCaseDetails = mock(UseCaseDetails.class);
        when(useCaseDetails.getDescription()).thenReturn("Test use case description");
        when(useCaseDetails.getNotes()).thenReturn("Test use case notes");
        when(useCaseDetails.getKeywords()).thenReturn(List.of("use case keyword1", "use case keyword2"));
        when(useCaseDetails.getConfiguration()).thenReturn("Test use case configuration");

        return List.of(useCaseDetails);
    }

    private List<MultiProcessorUseCaseDetails> getMultiProcessorUseCases() {
        final ProcessorConfigurationDetails configurationDetails1 = mock(ProcessorConfigurationDetails.class);
        when(configurationDetails1.getProcessorType()).thenReturn("Test processor type 1");
        when(configurationDetails1.getConfiguration()).thenReturn("Test configuration 1");

        final ProcessorConfigurationDetails configurationDetails2 = mock(ProcessorConfigurationDetails.class);
        when(configurationDetails2.getProcessorType()).thenReturn("Test processor type 2");
        when(configurationDetails2.getConfiguration()).thenReturn("Test configuration 2");

        final MultiProcessorUseCaseDetails useCaseDetails1 = mock(MultiProcessorUseCaseDetails.class);
        when(useCaseDetails1.getDescription()).thenReturn("Test description 1");
        when(useCaseDetails1.getNotes()).thenReturn("Test notes 1");
        when(useCaseDetails1.getKeywords()).thenReturn(List.of("keyword1", "keyword2"));
        when(useCaseDetails1.getConfigurations()).thenReturn(List.of(configurationDetails1, configurationDetails2));

        final MultiProcessorUseCaseDetails useCaseDetails2 = mock(MultiProcessorUseCaseDetails.class);
        when(useCaseDetails2.getDescription()).thenReturn("Test description 2");
        when(useCaseDetails2.getNotes()).thenReturn("Test notes 2");
        when(useCaseDetails2.getKeywords()).thenReturn(List.of("keyword3", "keyword4"));
        when(useCaseDetails2.getConfigurations()).thenReturn(List.of(configurationDetails1, configurationDetails2));

        return List.of(useCaseDetails1, useCaseDetails2);
    }

    private List<PropertyDescription> getPropertyDescriptions() {
        final PropertyDescription description1 = mock(PropertyDescription.class);
        when(description1.getName()).thenReturn("Property Description 1");
        when(description1.getDisplayName()).thenReturn("Property Description Display name 1");
        when(description1.getDescription()).thenReturn("This is a test description for Property Description 1");
        when(description1.getExpressionLanguageScope()).thenReturn("FLOWFILE_ATTRIBUTES");
        when(description1.getDefaultValue()).thenReturn("Test default value 1");
        when(description1.isRequired()).thenReturn(true);
        when(description1.isSensitive()).thenReturn(false);

        final PropertyDescription description2 = mock(PropertyDescription.class);
        when(description2.getName()).thenReturn("Property Description 2");
        when(description2.getDisplayName()).thenReturn("Property Description Display name 2");
        when(description2.getDescription()).thenReturn("This is a test description for Property Description 2");
        when(description2.getExpressionLanguageScope()).thenReturn("ENVIRONMENT");
        when(description2.getDefaultValue()).thenReturn("Test default value 2");
        when(description2.isRequired()).thenReturn(false);
        when(description2.isSensitive()).thenReturn(true);

        final PropertyDescription description3 = mock(PropertyDescription.class);
        when(description3.getName()).thenReturn("Property Description 3");
        when(description3.getDisplayName()).thenReturn("Property Description Display name 3");
        when(description3.getDescription()).thenReturn("This is a test description for Property Description 3");
        when(description3.getExpressionLanguageScope()).thenReturn("NONE");
        when(description3.getDefaultValue()).thenReturn("Test default value 3");
        when(description3.isRequired()).thenReturn(true);
        when(description3.isSensitive()).thenReturn(true);

        return List.of(description1, description2, description3);
    }

}
