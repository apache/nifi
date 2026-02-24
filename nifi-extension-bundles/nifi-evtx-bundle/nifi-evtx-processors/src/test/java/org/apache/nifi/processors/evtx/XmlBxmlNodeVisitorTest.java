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

package org.apache.nifi.processors.evtx;

import org.apache.nifi.processors.evtx.parser.BxmlNodeVisitor;
import org.apache.nifi.processors.evtx.parser.bxml.AttributeNode;
import org.apache.nifi.processors.evtx.parser.bxml.BxmlNode;
import org.apache.nifi.processors.evtx.parser.bxml.CDataSectionNode;
import org.apache.nifi.processors.evtx.parser.bxml.ConditionalSubstitutionNode;
import org.apache.nifi.processors.evtx.parser.bxml.EntityReferenceNode;
import org.apache.nifi.processors.evtx.parser.bxml.NormalSubstitutionNode;
import org.apache.nifi.processors.evtx.parser.bxml.OpenStartElementNode;
import org.apache.nifi.processors.evtx.parser.bxml.RootNode;
import org.apache.nifi.processors.evtx.parser.bxml.TemplateInstanceNode;
import org.apache.nifi.processors.evtx.parser.bxml.TemplateNode;
import org.apache.nifi.processors.evtx.parser.bxml.ValueNode;
import org.apache.nifi.processors.evtx.parser.bxml.value.BXmlTypeNode;
import org.apache.nifi.processors.evtx.parser.bxml.value.VariantTypeNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class XmlBxmlNodeVisitorTest {
    @Mock
    private XMLStreamWriter xmlStreamWriter;

    @Mock
    private RootNode rootNode;

    @Mock
    private BxmlNode bxmlNode;
    private List<VariantTypeNode> substitutions;
    private List<BxmlNode> children;
    private XmlBxmlNodeVisitor xmlBxmlNodeVisitor;

    @BeforeEach
    public void setup() throws IOException {
        substitutions = new ArrayList<>();
        children = new ArrayList<>(Arrays.asList(bxmlNode));
        when(rootNode.getSubstitutions()).thenReturn(substitutions);
        when(rootNode.getChildren()).thenReturn(children);
        xmlBxmlNodeVisitor = new XmlBxmlNodeVisitor(xmlStreamWriter, rootNode);
    }

    @Test
    public void testConstructor() throws IOException {
        verify(bxmlNode).accept(xmlBxmlNodeVisitor);
    }

    @Test
    public void testVisitOpenStartElementNode() throws IOException, XMLStreamException {
        final String tagName = "open";
        final OpenStartElementNode openStartElementNode = mock(OpenStartElementNode.class);
        final AttributeNode attributeNode = mock(AttributeNode.class);
        final AttributeNode attributeNode2 = mock(AttributeNode.class);
        final BxmlNode bxmlNode = mock(BxmlNode.class);

        when(openStartElementNode.getTagName()).thenReturn(tagName);
        when(openStartElementNode.getChildren()).thenReturn(Arrays.asList(attributeNode, bxmlNode, attributeNode2));

        xmlBxmlNodeVisitor.visit(openStartElementNode);

        final InOrder inOrder = inOrder(xmlStreamWriter, attributeNode, attributeNode2, bxmlNode);
        inOrder.verify(xmlStreamWriter).writeStartElement(tagName);
        inOrder.verify(attributeNode).accept(xmlBxmlNodeVisitor);
        inOrder.verify(attributeNode2).accept(xmlBxmlNodeVisitor);
        inOrder.verify(bxmlNode).accept(xmlBxmlNodeVisitor);
        inOrder.verify(xmlStreamWriter).writeEndElement();
    }

    @Test
    public void testVisitAttributeNodeValueType() throws IOException, XMLStreamException {
        final String attributeName = "attributeName";

        final AttributeNode attributeNode = mock(AttributeNode.class);
        final ValueNode valueNode = mock(ValueNode.class);
        final BxmlNode child = mock(BxmlNode.class);

        when(attributeNode.getAttributeName()).thenReturn(attributeName);
        when(attributeNode.getValue()).thenReturn(valueNode);
        when(valueNode.getChildren()).thenReturn(Arrays.asList(child));
        doAnswer(invocation -> {
            ((BxmlNodeVisitor) invocation.getArguments()[0]).visit(valueNode);
            return null;
        }).when(valueNode).accept(any(BxmlNodeVisitor.class));

        xmlBxmlNodeVisitor.visit(attributeNode);

        verify(xmlStreamWriter).writeAttribute(attributeName, null);
        verify(child).accept(any(BxmlNodeVisitor.class));
    }

    @Test
    public void testVisitAttributeNodeVariantType() throws IOException, XMLStreamException {
        final String attributeName = "attributeName";
        final String attributeValue = "attributeValue";

        final AttributeNode attributeNode = mock(AttributeNode.class);
        final VariantTypeNode variantTypeNode = mock(VariantTypeNode.class);

        when(attributeNode.getAttributeName()).thenReturn(attributeName);
        when(attributeNode.getValue()).thenReturn(variantTypeNode);
        doAnswer(invocation -> {
            ((BxmlNodeVisitor) invocation.getArguments()[0]).visit(variantTypeNode);
            return null;
        }).when(variantTypeNode).accept(any(BxmlNodeVisitor.class));
        when(variantTypeNode.getValue()).thenReturn(attributeValue);

        xmlBxmlNodeVisitor.visit(attributeNode);

        verify(xmlStreamWriter).writeAttribute(attributeName, attributeValue);
    }

    @Test
    public void testVisitAttributeNormalSubstitutionNode() throws IOException, XMLStreamException {
        final String attributeName = "attributeName";
        final String attributeValue = "attributeValue";

        final VariantTypeNode sub = mock(VariantTypeNode.class);
        when(sub.getValue()).thenReturn(attributeValue);
        substitutions.add(sub);

        final AttributeNode attributeNode = mock(AttributeNode.class);
        final NormalSubstitutionNode normalSubstitutionNode = mock(NormalSubstitutionNode.class);

        when(attributeNode.getAttributeName()).thenReturn(attributeName);
        when(attributeNode.getValue()).thenReturn(normalSubstitutionNode);
        doAnswer(invocation -> {
            ((BxmlNodeVisitor) invocation.getArguments()[0]).visit(normalSubstitutionNode);
            return null;
        }).when(normalSubstitutionNode).accept(any(BxmlNodeVisitor.class));
        when(normalSubstitutionNode.getIndex()).thenReturn(0);

        xmlBxmlNodeVisitor.visit(attributeNode);

        verify(xmlStreamWriter).writeAttribute(attributeName, attributeValue);
    }

    @Test
    public void testVisitAttributeConditionalSubstitutionNode() throws IOException, XMLStreamException {
        final String attributeName = "attributeName";
        final String attributeValue = "attributeValue";

        final VariantTypeNode sub = mock(VariantTypeNode.class);
        when(sub.getValue()).thenReturn(attributeValue);
        substitutions.add(sub);

        final AttributeNode attributeNode = mock(AttributeNode.class);
        final ConditionalSubstitutionNode conditionalSubstitutionNode = mock(ConditionalSubstitutionNode.class);

        when(attributeNode.getAttributeName()).thenReturn(attributeName);
        when(attributeNode.getValue()).thenReturn(conditionalSubstitutionNode);
        doAnswer(invocation -> {
            ((BxmlNodeVisitor) invocation.getArguments()[0]).visit(conditionalSubstitutionNode);
            return null;
        }).when(conditionalSubstitutionNode).accept(any(BxmlNodeVisitor.class));
        when(conditionalSubstitutionNode.getIndex()).thenReturn(0);

        xmlBxmlNodeVisitor.visit(attributeNode);

        verify(xmlStreamWriter).writeAttribute(attributeName, attributeValue);
    }

    @Test
    public void testVisitTemplateInstanceNode() throws IOException {
        final TemplateInstanceNode templateInstanceNode = mock(TemplateInstanceNode.class);
        final TemplateNode templateNode = mock(TemplateNode.class);

        when(templateInstanceNode.getTemplateNode()).thenReturn(templateNode);

        xmlBxmlNodeVisitor.visit(templateInstanceNode);
        verify(templateNode).accept(xmlBxmlNodeVisitor);
    }

    @Test
    public void testVisitTemplateNode() throws IOException {
        final TemplateNode templateNode = mock(TemplateNode.class);
        final BxmlNode child = mock(BxmlNode.class);

        when(templateNode.getChildren()).thenReturn(Arrays.asList(child));

        xmlBxmlNodeVisitor.visit(templateNode);

        verify(child).accept(xmlBxmlNodeVisitor);
    }

    @Test
    public void testVisitCDataSectionNode() throws IOException, XMLStreamException {
        final String cdata = "cdata";
        final CDataSectionNode cDataSectionNode = mock(CDataSectionNode.class);

        when(cDataSectionNode.getCdata()).thenReturn(cdata);

        xmlBxmlNodeVisitor.visit(cDataSectionNode);

        verify(xmlStreamWriter).writeCData(cdata);
    }

    @Test
    public void testVisitEntityReferenceNode() throws IOException, XMLStreamException {
        final String value = "value";
        final EntityReferenceNode entityReferenceNode = mock(EntityReferenceNode.class);

        when(entityReferenceNode.getValue()).thenReturn(value);

        xmlBxmlNodeVisitor.visit(entityReferenceNode);

        verify(xmlStreamWriter).writeCharacters(value);
    }

    @Test
    public void testVisitValueNode() throws IOException {
        final ValueNode valueNode = mock(ValueNode.class);
        final BxmlNode child = mock(BxmlNode.class);

        when(valueNode.getChildren()).thenReturn(Arrays.asList(child));

        xmlBxmlNodeVisitor.visit(valueNode);

        verify(child).accept(xmlBxmlNodeVisitor);
    }

    @Test
    public void testVisitConditionalSubstitutionNode() throws IOException {
        final ConditionalSubstitutionNode conditionalSubstitutionNode = mock(ConditionalSubstitutionNode.class);
        final VariantTypeNode sub = mock(VariantTypeNode.class);

        substitutions.add(sub);
        when(conditionalSubstitutionNode.getIndex()).thenReturn(0);

        xmlBxmlNodeVisitor.visit(conditionalSubstitutionNode);

        verify(sub).accept(xmlBxmlNodeVisitor);
    }

    @Test
    public void testVisitNormalSubstitutionNode() throws IOException {
        final NormalSubstitutionNode normalSubstitutionNode = mock(NormalSubstitutionNode.class);
        final VariantTypeNode sub = mock(VariantTypeNode.class);

        substitutions.add(sub);
        when(normalSubstitutionNode.getIndex()).thenReturn(0);

        xmlBxmlNodeVisitor.visit(normalSubstitutionNode);

        verify(sub).accept(xmlBxmlNodeVisitor);
    }

    @Test
    public void testVisitBxmlTypeNode() throws IOException {
        final BXmlTypeNode bXmlTypeNode = mock(BXmlTypeNode.class);
        final RootNode rootNode = mock(RootNode.class);

        when(bXmlTypeNode.getRootNode()).thenReturn(rootNode);

        xmlBxmlNodeVisitor.visit(bXmlTypeNode);

        verify(rootNode).accept(xmlBxmlNodeVisitor);
    }

    @Test
    public void testVisitVariantTypeNode() throws IOException, XMLStreamException {
        final String variantValue = "variantValue";
        final VariantTypeNode variantTypeNode = mock(VariantTypeNode.class);

        when(variantTypeNode.getValue()).thenReturn(variantValue);

        xmlBxmlNodeVisitor.visit(variantTypeNode);

        verify(xmlStreamWriter).writeCharacters(variantValue);
    }

    @Test
    public void testVisitRootNode() throws IOException {
        final RootNode rootNode = mock(RootNode.class);
        final BxmlNode child = mock(BxmlNode.class);

        when(rootNode.getChildren()).thenReturn(Arrays.asList(child));

        xmlBxmlNodeVisitor.visit(rootNode);

        final ArgumentCaptor<BxmlNodeVisitor> captor = ArgumentCaptor.forClass(BxmlNodeVisitor.class);
        verify(child).accept(captor.capture());

        final BxmlNodeVisitor value = captor.getValue();
        assertInstanceOf(XmlBxmlNodeVisitor.class, value);
        assertNotEquals(xmlBxmlNodeVisitor, value);
    }
}
