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
package org.apache.nifi.extension.manifest.parser.jaxb;

import org.apache.nifi.extension.manifest.ExtensionManifest;
import org.apache.nifi.extension.manifest.parser.ExtensionManifestException;
import org.apache.nifi.extension.manifest.parser.ExtensionManifestParser;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import java.io.InputStream;

public class JAXBExtensionManifestParser implements ExtensionManifestParser {

    private final Unmarshaller unmarshaller;

    public JAXBExtensionManifestParser() {
        try {
            final JAXBContext jaxbContext = JAXBContext.newInstance(ExtensionManifest.class);
            this.unmarshaller = jaxbContext.createUnmarshaller();
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to create JAXBContext: " + e.getMessage(), e);
        }
    }

    @Override
    public ExtensionManifest parse(final InputStream inputStream) {
        try {
            final JAXBElement<ExtensionManifest> jaxbElement = unmarshaller.unmarshal(new StreamSource(inputStream), ExtensionManifest.class);
            return jaxbElement.getValue();
        } catch (final JAXBException e) {
            throw new ExtensionManifestException(e.getMessage(), e);
        }
    }

}
