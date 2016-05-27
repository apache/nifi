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

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Visitor that writes an event to the XMLStreamWriter
 */
public class XmlBxmlNodeVisitor implements BxmlNodeVisitor {
    private final XMLStreamWriter xmlStreamWriter;
    private List<VariantTypeNode> substitutions;

    public XmlBxmlNodeVisitor(XMLStreamWriter xmlStreamWriter, RootNode rootNode) throws IOException {
        this.xmlStreamWriter = xmlStreamWriter;
        substitutions = rootNode.getSubstitutions();
        for (BxmlNode bxmlNode : rootNode.getChildren()) {
            bxmlNode.accept(this);
        }
    }

    @Override
    public void visit(OpenStartElementNode openStartElementNode) throws IOException {
        try {
            xmlStreamWriter.writeStartElement(openStartElementNode.getTagName());
            try {
                List<BxmlNode> nonAttributeChildren = new ArrayList<>();
                for (BxmlNode bxmlNode : openStartElementNode.getChildren()) {
                    if (bxmlNode instanceof AttributeNode) {
                        bxmlNode.accept(this);
                    } else {
                        nonAttributeChildren.add(bxmlNode);
                    }
                }
                for (BxmlNode nonAttributeChild : nonAttributeChildren) {
                    nonAttributeChild.accept(this);
                }
            } finally {
                xmlStreamWriter.writeEndElement();
            }
        } catch (XMLStreamException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void visit(AttributeNode attributeNode) throws IOException {
        try {
            AttributeNodeVisitor attributeNodeVisitor = new AttributeNodeVisitor();
            attributeNodeVisitor.visit(attributeNode);
            xmlStreamWriter.writeAttribute(attributeNode.getAttributeName(), attributeNodeVisitor.getValue());
        } catch (XMLStreamException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void visit(TemplateInstanceNode templateInstanceNode) throws IOException {
        templateInstanceNode.getTemplateNode().accept(this);
    }

    @Override
    public void visit(TemplateNode templateNode) throws IOException {
        for (BxmlNode bxmlNode : templateNode.getChildren()) {
            bxmlNode.accept(this);
        }
    }

    @Override
    public void visit(RootNode rootNode) throws IOException {
        new XmlBxmlNodeVisitor(xmlStreamWriter, rootNode);
    }

    @Override
    public void visit(CDataSectionNode cDataSectionNode) throws IOException {
        try {
            xmlStreamWriter.writeCData(cDataSectionNode.getCdata());
        } catch (XMLStreamException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void visit(EntityReferenceNode entityReferenceNode) throws IOException {
        try {
            xmlStreamWriter.writeCharacters(entityReferenceNode.getValue());
        } catch (XMLStreamException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void visit(ValueNode valueNode) throws IOException {
        for (BxmlNode bxmlNode : valueNode.getChildren()) {
            bxmlNode.accept(this);
        }
    }

    @Override
    public void visit(ConditionalSubstitutionNode conditionalSubstitutionNode) throws IOException {
        substitutions.get(conditionalSubstitutionNode.getIndex()).accept(this);
    }

    @Override
    public void visit(NormalSubstitutionNode normalSubstitutionNode) throws IOException {
        substitutions.get(normalSubstitutionNode.getIndex()).accept(this);
    }

    @Override
    public void visit(VariantTypeNode variantTypeNode) throws IOException {
        try {
            if (variantTypeNode instanceof BXmlTypeNode) {
                ((BXmlTypeNode) variantTypeNode).getRootNode().accept(this);
            } else {
                xmlStreamWriter.writeCharacters(variantTypeNode.getValue());
            }
        } catch (XMLStreamException e) {
            throw new IOException(e);
        }
    }

    private class AttributeNodeVisitor implements BxmlNodeVisitor {
        private String value;

        public String getValue() {
            return value;
        }

        @Override
        public void visit(AttributeNode attributeNode) throws IOException {
            attributeNode.getValue().accept(this);
        }

        @Override
        public void visit(ValueNode valueNode) throws IOException {
            for (BxmlNode bxmlNode : valueNode.getChildren()) {
                bxmlNode.accept(this);
            }
        }

        @Override
        public void visit(VariantTypeNode variantTypeNode) throws IOException {
            value = variantTypeNode.getValue();
        }

        @Override
        public void visit(NormalSubstitutionNode normalSubstitutionNode) throws IOException {
            value = substitutions.get(normalSubstitutionNode.getIndex()).getValue();
        }

        @Override
        public void visit(ConditionalSubstitutionNode conditionalSubstitutionNode) throws IOException {
            value = substitutions.get(conditionalSubstitutionNode.getIndex()).getValue();
        }
    }
}
