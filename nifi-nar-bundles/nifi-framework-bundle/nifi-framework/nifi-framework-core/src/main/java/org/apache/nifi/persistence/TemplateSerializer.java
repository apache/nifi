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
package org.apache.nifi.persistence;

import org.apache.nifi.controller.serialization.FlowSerializationException;
import org.apache.nifi.nar.NarClassLoaders;
import org.apache.nifi.web.api.dto.TemplateDTO;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public final class TemplateSerializer {

    public static byte[] serialize(final TemplateDTO dto) {
        final ClassLoader currentCl = Thread.currentThread().getContextClassLoader();
        final ClassLoader cl = NarClassLoaders.getFrameworkClassLoader();
        Thread.currentThread().setContextClassLoader(cl);
        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final BufferedOutputStream bos = new BufferedOutputStream(baos);

            JAXBContext context = JAXBContext.newInstance(TemplateDTO.class);
            Marshaller marshaller = context.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
            marshaller.marshal(dto, bos);

            bos.flush();
            return baos.toByteArray();
        } catch (final IOException | JAXBException e) {
            throw new FlowSerializationException(e);
        } finally {
            if (currentCl != null) {
                Thread.currentThread().setContextClassLoader(currentCl);
            }
        }
    }

}
