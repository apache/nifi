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
package org.apache.nifi.processors.soap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.mockserver.mock.action.ExpectationCallback;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * This callback is used to verify that the request sent to the soap server is valid XML.
 */
public class VerifyRequestCallback implements ExpectationCallback {

    private static final Logger logger = LoggerFactory.getLogger(VerifyRequestCallback.class);

    private final String xmlBody =
            "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>\n" +
            "<SOAP-ENV:Envelope SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\" xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:SOAP-ENC=\"http://schemas.xmlsoap.org/soap/encoding/\">\n" +
            "    <SOAP-ENV:Body>\n" +
            "        <ns1:LatLonListZipCodeResponse xmlns:ns1=\"http://graphical.weather.gov/xml/DWMLgen/wsdl/ndfdXML.wsdl\">\n" +
            "            <listLatLonOut xsi:type=\"xsd:string\"></listLatLonOut>\n" +
            "        </ns1:LatLonListZipCodeResponse>\n" +
            "    </SOAP-ENV:Body>\n" +
            "</SOAP-ENV:Envelope>";

    public VerifyRequestCallback() {
    }

    @Override
    public HttpResponse handle(HttpRequest httpRequest) {
        String soapRequest = httpRequest.getBodyAsString();
        logger.info("PATH: {}", httpRequest.getPath().getValue());
        XmlMapper mapper = new XmlMapper();
        try {
            mapper.readValue(soapRequest, new TypeReference<Map<String, Object>>() {
            });
        }
        catch (IOException e) {
            logger.error("REQUEST: " + soapRequest);
            logger.error(e.getMessage(), e);
            return HttpResponse.notFoundResponse();
        }
        return HttpResponse.response(xmlBody);
    }
}
