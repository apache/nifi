/*
` * Licensed to the Apache Software Foundation (ASF) under one or more
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

package org.apache.nifi.cluster.coordination.http.replication.okhttp;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

public class XmlEntitySerializer implements EntitySerializer {
    private final ConcurrentMap<Class<?>, JAXBContext> jaxbContextCache = new ConcurrentHashMap<>();

    @Override
    public void serialize(final Object entity, final OutputStream out) throws IOException {
        try {
            final Marshaller marshaller = getJaxbContext(entity.getClass()).createMarshaller();
            marshaller.marshal(entity, out);
        } catch (final JAXBException e) {
            throw new IOException(e);
        }
    }

    private JAXBContext getJaxbContext(final Class<?> entityType) {
        JAXBContext context = jaxbContextCache.get(entityType);
        if (context != null) {
            return context;
        }

        context = createJaxbContext(entityType);
        jaxbContextCache.putIfAbsent(entityType, context);
        return context;
    }

    private JAXBContext createJaxbContext(final Class<?> entityType) {
        try {
            return JAXBContext.newInstance(entityType);
        } catch (final JAXBException e) {
            throw new RuntimeException("Failed to create JAXBContext for Entity Type [" + entityType + "] so could not parse incoming request", e);
        }
    }
}
