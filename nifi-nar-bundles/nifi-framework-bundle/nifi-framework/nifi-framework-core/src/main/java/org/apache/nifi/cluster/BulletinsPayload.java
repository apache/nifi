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
package org.apache.nifi.cluster;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.apache.nifi.cluster.protocol.ProtocolException;
import org.apache.nifi.jaxb.BulletinAdapter;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.security.xml.XmlUtils;

/**
 * The payload of the bulletins.
 *
 */
@XmlRootElement
public class BulletinsPayload {

    private static final JAXBContext JAXB_CONTEXT;

    static {
        try {
            JAXB_CONTEXT = JAXBContext.newInstance(BulletinsPayload.class);
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to create JAXBContext.");
        }
    }

    private Set<Bulletin> bulletins;

    @XmlJavaTypeAdapter(BulletinAdapter.class)
    public Set<Bulletin> getBulletins() {
        return bulletins;
    }

    public void setBulletins(final Set<Bulletin> bulletins) {
        this.bulletins = bulletins;
    }

    public byte[] marshal() throws ProtocolException {
        final ByteArrayOutputStream payloadBytes = new ByteArrayOutputStream();
        marshal(this, payloadBytes);
        return payloadBytes.toByteArray();
    }

    public static void marshal(final BulletinsPayload payload, final OutputStream os) throws ProtocolException {
        try {
            final Marshaller marshaller = JAXB_CONTEXT.createMarshaller();
            marshaller.marshal(payload, os);
        } catch (final JAXBException je) {
            throw new ProtocolException(je);
        }
    }

    public static BulletinsPayload unmarshal(final InputStream is) throws ProtocolException {
        try {
            final Unmarshaller unmarshaller = JAXB_CONTEXT.createUnmarshaller();
            final XMLStreamReader xsr = XmlUtils.createSafeReader(is);
            return (BulletinsPayload) unmarshaller.unmarshal(xsr);
        } catch (final JAXBException | XMLStreamException e) {
            throw new ProtocolException(e);
        }
    }

    public static BulletinsPayload unmarshal(final byte[] bytes) throws ProtocolException {
        return unmarshal(new ByteArrayInputStream(bytes));
    }
}
