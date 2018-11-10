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



package org.apache.nifi.testharness;

import org.apache.nifi.processor.Processor;
import org.apache.nifi.testharness.api.FlowFileEditorCallback;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.util.LinkedList;
import java.util.Objects;


/**
 * <p>
 * A facility to describe simple, common changes to a NiFi flow before it is installed to the test
 * NiFi instance. Intended to be used by
 * {@link TestNiFiInstance.Builder#modifyFlowXmlBeforeInstalling(FlowFileEditorCallback)}
 * </p>
 *
 * <p>
 * The desired edits can be configured via the {@link Builder} object returned by the {@link #builder()}
 * method. Once fully configured, the {@link Builder#build()} emits a {@code FlowFileEditorCallback}
 * object that can be passed to
 * {@link TestNiFiInstance.Builder#modifyFlowXmlBeforeInstalling(FlowFileEditorCallback)}.
 * </p>
 *
 * <p>
 * <strong>CAUTION: THIS IS AN EXPERIMENTAL API: EXPECT CHANGES!</strong>
 * Efforts will be made to retain backwards API compatibility, but
 * no guarantee is given.
 * </p>
 *
 * @see TestNiFiInstance.Builder#modifyFlowXmlBeforeInstalling(FlowFileEditorCallback)
 *
 */

public final class SimpleNiFiFlowDefinitionEditor implements FlowFileEditorCallback, TestNiFiInstanceAware {


    private final LinkedList<FlowFileEditorCallback> delegateActions;
    private TestNiFiInstance testNiFiInstance;

    private SimpleNiFiFlowDefinitionEditor(LinkedList<FlowFileEditorCallback> delegateActions) {
        this.delegateActions = delegateActions;
    }

    @Override
    public Document edit(Document document) throws Exception {

        for (FlowFileEditorCallback change : delegateActions) {
            if (change instanceof TestNiFiInstanceAware) {
                ((TestNiFiInstanceAware)change).setTestNiFiInstance(testNiFiInstance);
            }

            document = change.edit(document);
        }

        return document;
    }

    @Override
    public void setTestNiFiInstance(TestNiFiInstance testNiFiInstance) {
        this.testNiFiInstance = Objects.requireNonNull(
                testNiFiInstance, "argument testNiFiInstance cannot be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private Builder() {
            // no external instance
        }

        private XPath xpath = XPathFactory.newInstance().newXPath();
        private final LinkedList<FlowFileEditorCallback> actions = new LinkedList<>();

        public Builder rawXmlChange(FlowFileEditorCallback flowFileEditorCallback) {
            actions.addLast(flowFileEditorCallback);
            return this;
        }

        public Builder setSingleProcessorProperty(String processorName, String propertyName, String newValue) {

            return rawXmlChange(document -> {
                String xpathString = "//processor[name/text() = '" + processorName
                        + "']/property[name/text() = '" + propertyName + "']/value";

                Node propertyValueNode = (Node) xpath.evaluate(xpathString, document, XPathConstants.NODE);

                if (propertyValueNode == null) {
                    throw new IllegalArgumentException("Reference to processor '"+ processorName +"' with property '"
                            + propertyName + "' not found: " + xpathString);
                }

                propertyValueNode.setTextContent(newValue);

                return document;
            });


        }

        public <P extends Processor> Builder setClassOfSingleProcessor(String processorName, Class<P> mockProcessor) {

            return setClassOfSingleProcessor(processorName, mockProcessor.getName());
        }

        public Builder setClassOfSingleProcessor(String processorName, String newFullyQualifiedClassName) {

            return rawXmlChange(document -> {
                String xpathString = "//processor[name/text() = '" + processorName + "']/class";

                Node classNameNode = (Node) xpath.evaluate(xpathString, document, XPathConstants.NODE);

                if (classNameNode == null) {
                    throw new IllegalArgumentException("Reference to processor '"+ processorName +" not found: " +
                            xpathString);
                }

                classNameNode.setTextContent(newFullyQualifiedClassName);

                return document;
            });
        }

        public Builder updateFlowFileBuiltInNiFiProcessorVersionsToNiFiVersion() {

            return rawXmlChange(new UpdateFlowFileNiFiVersionFlowFileEditorCallback());
        }


        public SimpleNiFiFlowDefinitionEditor build() {
            return new SimpleNiFiFlowDefinitionEditor(actions);
        }

    }


    private static final class UpdateFlowFileNiFiVersionFlowFileEditorCallback
            implements FlowFileEditorCallback, TestNiFiInstanceAware {

        private TestNiFiInstance testNiFiInstance;

        @Override
        public Document edit(Document document) throws Exception {
            String niFiVersion = getNiFiVersion();

            XPath xpath = XPathFactory.newInstance().newXPath();

            NodeList processorNodeVersionList = (NodeList)
                    xpath.evaluate("//bundle/group[text() = \"org.apache.nifi\"]/parent::bundle/version",
                            document, XPathConstants.NODESET);

            final int length = processorNodeVersionList.getLength();
            for (int i=0; i<length; i++) {
                Node processorNodeVersion = processorNodeVersionList.item(i);

                processorNodeVersion.setTextContent(niFiVersion);
            }

            return document;
        }

        private String getNiFiVersion() {
            return Objects.requireNonNull(
                    testNiFiInstance, "testNiFiInstance cannot be null").getNifiVersion();
        }

        @Override
        public void setTestNiFiInstance(TestNiFiInstance testNiFiInstance) {
            this.testNiFiInstance = Objects.requireNonNull(
                    testNiFiInstance, "argument testNiFiInstance cannot be null");

        }


    }
}
