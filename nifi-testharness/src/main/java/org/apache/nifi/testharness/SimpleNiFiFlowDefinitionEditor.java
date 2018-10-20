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

import org.apache.nifi.testharness.api.FlowFileEditorCallback;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.util.LinkedList;


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

public final class SimpleNiFiFlowDefinitionEditor implements FlowFileEditorCallback {


    private final LinkedList<FlowFileEditorCallback> delegateActions;

    private SimpleNiFiFlowDefinitionEditor(LinkedList<FlowFileEditorCallback> delegateActions) {
        this.delegateActions = delegateActions;
    }

    @Override
    public Document edit(Document document) throws Exception {

        for (FlowFileEditorCallback change : delegateActions) {
            document = change.edit(document);
        }

        return document;
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

        public Builder setClassOfSingleProcessor(String processorName, Class<?> mockProcessor) {

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


        public SimpleNiFiFlowDefinitionEditor build() {
            return new SimpleNiFiFlowDefinitionEditor(actions);
        }

    }
}
