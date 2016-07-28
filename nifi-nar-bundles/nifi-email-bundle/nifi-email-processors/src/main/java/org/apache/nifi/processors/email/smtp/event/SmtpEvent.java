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

package org.apache.nifi.processors.email.smtp.event;



import java.io.InputStream;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Smtp event which adds the transaction number and command to the StandardEvent.
 */

public class SmtpEvent{
    private final String remoteIP;
    private final String helo;
    private final String from;
    private final String to;
    private final InputStream messageData;
    private List<Map<String, String>> certificatesDetails;
    private AtomicBoolean processed = new AtomicBoolean(false);
    private AtomicBoolean acknowledged = new AtomicBoolean(false);
    private AtomicInteger returnCode = new AtomicInteger();

    public SmtpEvent(
            final String remoteIP, final String helo, final String from, final String to, final X509Certificate[] certificates,
            final InputStream messageData) {

        this.remoteIP = remoteIP;
        this.helo = helo;
        this.from = from;
        this.to = to;
        this.messageData = messageData;

        this.certificatesDetails = new ArrayList<>();

        for (int c = 0; c < certificates.length; c++) {
            X509Certificate cert = certificates[c];
            if (cert.getSerialNumber() != null && cert.getSubjectDN() != null) {
                Map<String, String> certificate = new HashMap<>();

                String certSerialNumber = cert.getSerialNumber().toString();
                String certSubjectDN = cert.getSubjectDN().getName();


                certificate.put("SerialNumber", certSerialNumber);
                certificate.put("SubjectName", certSubjectDN);

                certificatesDetails.add(certificate);

            }
        }
    }

    public synchronized List<Map<String, String>> getCertifcateDetails() {
        return certificatesDetails;
    }

    public synchronized String getHelo() {
        return helo;
    }

    public synchronized InputStream getMessageData() {
        return messageData;
    }

    public synchronized String getFrom() {
        return from;
    }

    public synchronized String getTo() {
        return to;
    }

    public synchronized String getRemoteIP() {
        return remoteIP;
    }

    public synchronized void setProcessed() {
        this.processed.set(true);
    }

    public synchronized boolean getProcessed() {
        return this.processed.get();
    }

    public synchronized void setAcknowledged() {
        this.acknowledged.set(true);
    }

    public synchronized boolean getAcknowledged() {
        return this.acknowledged.get();
    }

    public synchronized void setReturnCode(int code) {
        this.returnCode.set(code);
    }

    public synchronized Integer getReturnCode() {
        return this.returnCode.get();
    }

}

