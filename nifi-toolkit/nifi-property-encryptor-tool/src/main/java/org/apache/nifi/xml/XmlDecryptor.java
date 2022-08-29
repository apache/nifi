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
package org.apache.nifi.xml;

import org.apache.nifi.properties.SensitivePropertyProviderFactory;
import org.apache.nifi.properties.scheme.ProtectionScheme;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

public class XmlDecryptor extends XmlCryptoParser {

    protected static final String ENCRYPTION_NONE = "none";
    protected static final String PROPERTY_ELEMENT = "property";
    protected static final String ENCRYPTION_ATTRIBUTE_NAME = "encryption";

    public XmlDecryptor(final SensitivePropertyProviderFactory providerFactory, final ProtectionScheme scheme) {
        super(providerFactory, scheme);
        this.providerFactory = providerFactory;
    }

    public void decrypt(final InputStream encryptedXmlContent, final OutputStream decryptedOutputStream) {
        cryptographicXmlOperation(encryptedXmlContent, decryptedOutputStream);
    }

    @Override
    protected StartElement updateStartElementEncryptionAttribute(XMLEvent xmlEvent) {
        return convertToDecryptedElement(xmlEvent);
    }

    @Override
    protected Characters cryptoOperationOnCharacters(XMLEvent xmlEvent, String groupIdentifier, final String propertyName) {
        return decryptElementCharacters(xmlEvent, groupIdentifier, propertyName);
    }

    /**
     * Decrypt the XMLEvent element characters/value, which should contain an encrypted value
     * @param xmlEvent The encrypted Characters event to be decrypted
     * @return The decrypted Characters event
     */
    private Characters decryptElementCharacters(final XMLEvent xmlEvent, final String groupIdentifier, final String propertyName) {
        final XMLEventFactory eventFactory = XMLEventFactory.newInstance();
        final String encryptedCharacters = xmlEvent.asCharacters().getData().trim();
        String decryptedCharacters = cryptoProvider.unprotect(encryptedCharacters, providerFactory.getPropertyContext(groupIdentifier, propertyName));
        return eventFactory.createCharacters(decryptedCharacters);
    }

    /**
     * Takes a StartElement and updates the 'encrypted' attribute to empty string to remove the encryption method/scheme
     * @param xmlEvent The opening/start XMLEvent for an encrypted property
     * @return The updated element to be written to XML file
     */
    private StartElement convertToDecryptedElement(final XMLEvent xmlEvent) {
        if (isEncryptedElement(xmlEvent)) {
            return updateElementAttribute(xmlEvent, ENCRYPTION_ATTRIBUTE_NAME, ENCRYPTION_NONE);
        } else {
            throw new XmlCryptoException(String.format("Failed to update the element's [%s] attribute when decrypting the element value", ENCRYPTION_ATTRIBUTE_NAME));
        }
    }

    private boolean isEncryptedElement(final XMLEvent xmlEvent) {
        return xmlEvent.isStartElement()
                && xmlEvent.asStartElement().getName().toString().equals(PROPERTY_ELEMENT)
                && elementHasEncryptionAttribute(xmlEvent.asStartElement());
    }

    private boolean elementHasEncryptionAttribute(final StartElement xmlEvent) {
        return xmlElementHasAttribute(xmlEvent, ENCRYPTION_ATTRIBUTE_NAME);
    }

    private boolean xmlElementHasAttribute(final StartElement xmlEvent, final String attributeName) {
        return !Objects.isNull(xmlEvent.getAttributeByName(new QName(attributeName)));
    }
}
