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

package org.apache.nifi.processors.email;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailAttachment;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.MultiPartEmail;
import org.apache.commons.mail.SimpleEmail;

public class GenerateAttachment {
    String from;
    String to;
    String subject;
    String message;
    String hostName;

    public GenerateAttachment(String from, String to, String subject, String message, String hostName) {
        this.from = from;
        this.to = to;
        this.subject = subject;
        this.message = message;
        this.hostName = hostName;
    }

    public byte[] SimpleEmail() {
        MimeMessage mimeMessage = SimpleEmailMimeMessage();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            mimeMessage.writeTo(output);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (MessagingException e) {
            e.printStackTrace();
        }

        return output.toByteArray();
    }

    public MimeMessage SimpleEmailMimeMessage() {
        Email email = new SimpleEmail();
        try {
            email.setFrom(from);
            email.addTo(to);
            email.setSubject(subject);
            email.setMsg(message);
            email.setHostName(hostName);
            email.buildMimeMessage();
        } catch (EmailException e) {
            e.printStackTrace();
        }

        return email.getMimeMessage();
    }


    public byte[] WithAttachments(int amount) {
        MultiPartEmail email = new MultiPartEmail();
        try {

            email.setFrom(from);
            email.addTo(to);
            email.setSubject(subject);
            email.setMsg(message);
            email.setHostName(hostName);

            int x = 1;
            while (x <= amount) {
                // Create an attachment with the pom.xml being used to compile (yay!!!)
                EmailAttachment attachment = new EmailAttachment();
                attachment.setPath("pom.xml");
                attachment.setDisposition(EmailAttachment.ATTACHMENT);
                attachment.setDescription("pom.xml");
                attachment.setName("pom.xml"+String.valueOf(x));
                //  attach
                email.attach(attachment);
                x++;
            }
            email.buildMimeMessage();
        } catch (EmailException e) {
            e.printStackTrace();
        }
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        MimeMessage mimeMessage = email.getMimeMessage();
        try {
            mimeMessage.writeTo(output);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (MessagingException e) {
            e.printStackTrace();
        }

        return output.toByteArray();
    }
}
