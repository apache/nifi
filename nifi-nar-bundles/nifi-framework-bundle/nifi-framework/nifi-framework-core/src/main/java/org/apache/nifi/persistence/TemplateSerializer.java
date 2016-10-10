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
import org.apache.nifi.web.api.dto.TemplateDTO;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import com.sun.xml.txw2.output.IndentingXMLStreamWriter;

public final class TemplateSerializer {

    /**
     * This method when called assumes the Framework Nar ClassLoader is in the
     * classloader hierarchy of the current context class loader.
     * @param dto the template dto to serialize
     * @return serialized representation of the DTO
     */
    public static byte[] serialize(final TemplateDTO dto) {
        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final BufferedOutputStream bos = new BufferedOutputStream(baos);

            JAXBContext context = JAXBContext.newInstance(TemplateDTO.class);
            Marshaller marshaller = context.createMarshaller();
            XMLOutputFactory xmlof = XMLOutputFactory.newInstance();
            XMLStreamWriter writer = new IndentingXMLStreamWriter(xmlof.createXMLStreamWriter(bos));
            marshaller.marshal(dto, writer);

            bos.flush();
            return baos.toByteArray(); //Note: For really large templates this could use a lot of heap space
        } catch (final IOException | JAXBException | XMLStreamException e) {
            throw new FlowSerializationException(e);
        }
    }

}
