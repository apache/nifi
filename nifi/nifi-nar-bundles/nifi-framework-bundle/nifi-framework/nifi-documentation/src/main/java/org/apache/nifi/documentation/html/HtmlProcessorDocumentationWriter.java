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

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;

/**
 * Writes documentation specific for a Processor. This includes everything for a
 * ConfigurableComponent as well as Relationship information.
 * 
 *
 */
public class HtmlProcessorDocumentationWriter extends HtmlDocumentationWriter {

	@Override
	protected void writeAdditionalBodyInfo(final ConfigurableComponent configurableComponent,
			final XMLStreamWriter xmlStreamWriter) throws XMLStreamException {
		final Processor processor = (Processor) configurableComponent;
		xmlStreamWriter.writeStartElement("p");
		writeSimpleElement(xmlStreamWriter, "strong", "Relationships: ");
		xmlStreamWriter.writeEndElement();

		if (processor.getRelationships().size() > 0) {
			xmlStreamWriter.writeStartElement("table");
			xmlStreamWriter.writeStartElement("tr");
			writeSimpleElement(xmlStreamWriter, "th", "Name");
			writeSimpleElement(xmlStreamWriter, "th", "Description");
			xmlStreamWriter.writeEndElement();

			for (Relationship relationship : processor.getRelationships()) {
				xmlStreamWriter.writeStartElement("tr");
				writeSimpleElement(xmlStreamWriter, "td", relationship.getName());
				writeSimpleElement(xmlStreamWriter, "td", relationship.getDescription());
				xmlStreamWriter.writeEndElement();
			}
			xmlStreamWriter.writeEndElement();
		} else {
			xmlStreamWriter.writeCharacters("This processor has no relationships.");
		}
	}
}
