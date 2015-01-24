package org.apache.nifi.documentation.html;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;

public class HtmlProcessorDocumentationWriter extends HtmlDocumentationWriter {

	@Override
	protected void writeBodySub(ConfigurableComponent configurableComponent, XMLStreamWriter xmlStreamWriter)
			throws XMLStreamException {
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
