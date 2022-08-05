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
import java.io.InputStream;
import java.io.OutputStream;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

public class XmlEncryptor extends XmlCryptoParser {

    public XmlEncryptor(final SensitivePropertyProviderFactory providerFactory, final ProtectionScheme scheme) {
        super(providerFactory, scheme);
        this.providerFactory = providerFactory;
    }

    public void encrypt(final InputStream encryptedXmlContent, final OutputStream decryptedOutputStream) {
        cryptographicXmlOperation(encryptedXmlContent, decryptedOutputStream);
    }

    @Override
    protected StartElement updateStartElementEncryptionAttribute(final XMLEvent xmlEvent) {
        return convertToEncryptedElement(xmlEvent, cryptoProvider.getIdentifierKey());
    }

    @Override
    protected Characters cryptoOperationOnCharacters(XMLEvent xmlEvent, String groupIdentifier, final String propertyName) {
        return encryptElementCharacters(xmlEvent, groupIdentifier, propertyName);
    }

    /**
     * Encrypt the XMLEvent element characters (the sensitive value)
     * @param xmlEvent The encrypted Characters event to be decrypted
     * @return The decrypted Characters event
     */
    private Characters encryptElementCharacters(final XMLEvent xmlEvent, final String groupIdentifier, final String propertyName) {
        final XMLEventFactory eventFactory = XMLEventFactory.newInstance();
        final String sensitiveCharacters = xmlEvent.asCharacters().getData().trim();
        String encryptedCharacters = cryptoProvider.protect(sensitiveCharacters, providerFactory.getPropertyContext(groupIdentifier, propertyName));
        return eventFactory.createCharacters(encryptedCharacters);
    }

    private StartElement convertToEncryptedElement(final XMLEvent xmlEvent, final String encryptionScheme) {
        return updateElementAttribute(xmlEvent, ENCRYPTION_ATTRIBUTE_NAME, encryptionScheme);
    }
}
