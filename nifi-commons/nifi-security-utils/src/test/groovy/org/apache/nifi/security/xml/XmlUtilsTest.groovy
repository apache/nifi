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
package org.apache.nifi.security.xml

import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.xml.bind.JAXBContext
import javax.xml.bind.UnmarshalException
import javax.xml.bind.Unmarshaller
import javax.xml.bind.annotation.XmlAccessType
import javax.xml.bind.annotation.XmlAccessorType
import javax.xml.bind.annotation.XmlAttribute
import javax.xml.bind.annotation.XmlRootElement
import javax.xml.stream.XMLStreamReader

import static groovy.test.GroovyAssert.shouldFail

@RunWith(JUnit4.class)
class XmlUtilsTest {
    private static final Logger logger = LoggerFactory.getLogger(XmlUtilsTest.class)

    @BeforeClass
    static void setUpOnce() throws Exception {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() throws Exception {

    }

    @After
    void tearDown() throws Exception {

    }

    @Test
    void testShouldHandleXXEInUnmarshal() {
        // Arrange
        final String XXE_TEMPLATE_FILEPATH = "src/test/resources/xxe_template.xml"
        InputStream templateStream = new File(XXE_TEMPLATE_FILEPATH).newInputStream()

        JAXBContext context = JAXBContext.newInstance(XmlObject.class)

        // Act
        def msg = shouldFail(UnmarshalException) {
            Unmarshaller unmarshaller = context.createUnmarshaller()
            XMLStreamReader xsr = XmlUtils.createSafeReader(templateStream)
            def parsed = unmarshaller.unmarshal(xsr, XmlObject.class)
            logger.info("Unmarshalled ${parsed.toString()}")
        }

        // Assert
        logger.expected(msg)
        assert msg =~ "XMLStreamException: ParseError "
    }
}

@XmlAccessorType( XmlAccessType.NONE )
@XmlRootElement(name = "object")
class XmlObject {
    @XmlAttribute
    String name

    @XmlAttribute
    String description

    @XmlAttribute
    String groupId

    @XmlAttribute
    String timestamp
}
