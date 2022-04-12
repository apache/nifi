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
package org.apache.nifi.xml.processing;

import java.io.InputStream;

public class ResourceProvider {
    private static final String STANDARD_DOCUMENT_DOCTYPE_ENTITY = "/standard-document-doctype-entity.xml";

    private static final String STANDARD_DOCUMENT_DOCTYPE = "/standard-document-doctype.xml";

    private static final String STANDARD_DOCUMENT = "/standard-document.xml";

    private static final String STANDARD_NAMESPACE_DOCUMENT = "/standard-namespace-document.xml";

    private static final String STANDARD_NAMESPACE_DOCUMENT_DOCTYPE_ENTITY = "/standard-namespace-document-doctype-entity.xml";

    private static final String STANDARD_SCHEMA = "/standard-schema.xsd";

    public static InputStream getStandardDocument() {
        return getResource(STANDARD_DOCUMENT);
    }

    public static InputStream getStandardDocumentDocTypeEntity() {
        return getResource(STANDARD_DOCUMENT_DOCTYPE_ENTITY);
    }

    public static InputStream getStandardDocumentDocType() {
        return getResource(STANDARD_DOCUMENT_DOCTYPE);
    }

    public static InputStream getStandardNamespaceDocument() {
        return getResource(STANDARD_NAMESPACE_DOCUMENT);
    }

    public static InputStream getStandardNamespaceDocumentDocTypeEntity() {
        return getResource(STANDARD_NAMESPACE_DOCUMENT_DOCTYPE_ENTITY);
    }

    public static InputStream getStandardSchema() {
        return getResource(STANDARD_SCHEMA);
    }

    private static InputStream getResource(final String path) {
        final InputStream resource = ResourceProvider.class.getResourceAsStream(path);
        if (resource == null) {
            throw new IllegalStateException(String.format("Resource [%s] not found", path));
        }
        return resource;
    }
}
