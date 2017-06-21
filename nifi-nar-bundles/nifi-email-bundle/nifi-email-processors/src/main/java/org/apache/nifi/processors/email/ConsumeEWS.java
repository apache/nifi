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

import microsoft.exchange.webservices.data.autodiscover.IAutodiscoverRedirectionUrl;
import microsoft.exchange.webservices.data.core.ExchangeService;
import microsoft.exchange.webservices.data.core.PropertySet;
import microsoft.exchange.webservices.data.core.enumeration.misc.ExchangeVersion;
import microsoft.exchange.webservices.data.core.enumeration.property.BodyType;
import microsoft.exchange.webservices.data.core.enumeration.property.WellKnownFolderName;
import microsoft.exchange.webservices.data.core.enumeration.search.FolderTraversal;
import microsoft.exchange.webservices.data.core.enumeration.search.LogicalOperator;
import microsoft.exchange.webservices.data.core.enumeration.search.SortDirection;
import microsoft.exchange.webservices.data.core.enumeration.service.ConflictResolutionMode;
import microsoft.exchange.webservices.data.core.enumeration.service.DeleteMode;
import microsoft.exchange.webservices.data.core.service.folder.Folder;
import microsoft.exchange.webservices.data.core.service.item.EmailMessage;
import microsoft.exchange.webservices.data.core.service.item.Item;
import microsoft.exchange.webservices.data.core.service.schema.EmailMessageSchema;
import microsoft.exchange.webservices.data.core.service.schema.FolderSchema;
import microsoft.exchange.webservices.data.core.service.schema.ItemSchema;
import microsoft.exchange.webservices.data.credential.ExchangeCredentials;
import microsoft.exchange.webservices.data.credential.WebCredentials;
import microsoft.exchange.webservices.data.property.complex.FileAttachment;
import microsoft.exchange.webservices.data.search.FindFoldersResults;
import microsoft.exchange.webservices.data.search.FindItemsResults;
import microsoft.exchange.webservices.data.search.FolderView;
import microsoft.exchange.webservices.data.search.ItemView;
import microsoft.exchange.webservices.data.search.filter.SearchFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.mail.EmailAttachment;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.apache.commons.mail.MultiPartEmail;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.Address;
import javax.mail.Flags;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import javax.mail.util.ByteArrayDataSource;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Consumes messages from Microsoft Exchange using Exchange Web Services. "
        + "The raw-bytes of each received email message are written as contents of the FlowFile")
@Tags({ "Email", "EWS", "Exchange", "Get", "Ingest", "Ingress", "Message", "Consume" })
public class ConsumeEWS extends AbstractProcessor {
    public static final PropertyDescriptor USER = new PropertyDescriptor.Builder()
            .name("user")
            .displayName("User Name")
            .description("User Name used for authentication and authorization with Email server.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("password")
            .displayName("Password")
            .description("Password used for authentication and authorization with Email server.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();
    public static final PropertyDescriptor FOLDER = new PropertyDescriptor.Builder()
            .name("folder")
            .displayName("Folder")
            .description("Email folder to retrieve messages from (e.g., INBOX)")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("INBOX")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("fetch.size")
            .displayName("Fetch Size")
            .description("Specify the maximum number of Messages to fetch per call to Email Server.")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("10")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    public static final PropertyDescriptor SHOULD_DELETE_MESSAGES = new PropertyDescriptor.Builder()
            .name("delete.messages")
            .displayName("Delete Messages")
            .description("Specify whether mail messages should be deleted after retrieval.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    static final PropertyDescriptor CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("connection.timeout")
            .displayName("Connection timeout")
            .description("The amount of time to wait to connect to Email server")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(true)
            .defaultValue("30 sec")
            .build();
    public static final PropertyDescriptor EXCHANGE_VERSION = new PropertyDescriptor.Builder()
            .name("mail-ews-version")
            .displayName("Exchange Version")
            .description("What version of Exchange Server the server is running.")
            .required(true)
            .allowableValues(ExchangeVersion.values())
            .defaultValue(ExchangeVersion.Exchange2010_SP2.name())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor EWS_URL = new PropertyDescriptor.Builder()
            .name("ews-url")
            .displayName("EWS URL")
            .description("URL of the EWS Endpoint. Required if Autodiscover is false.")
            .required(false)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor USE_AUTODISCOVER = new PropertyDescriptor.Builder()
            .name("ews-autodiscover")
            .displayName("Auto Discover URL")
            .description("Whether or not to use the Exchange email address to Autodiscover the EWS endpoint URL.")
            .required(true)
            .allowableValues("true","false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor SHOULD_MARK_READ = new PropertyDescriptor.Builder()
            .name("ews-mark-as-read")
            .displayName("Mark Messages as Read")
            .description("Specify if messages should be marked as read after retrieval.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor INCLUDE_EMAIL_HEADERS = new PropertyDescriptor.Builder()
            .name("ews-include-headers")
            .displayName("Original Headers to Include")
            .description("Comma delimited list specifying which headers from the original message to include in the exported email message. Blank means copy all headers. " +
                    "Some headers can cause problems with message parsing, specifically the 'Content-Type' header.")
            .defaultValue("")
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor EXCLUDE_EMAIL_HEADERS = new PropertyDescriptor.Builder()
            .name("ews-exclude-headers")
            .displayName("Original Headers to Exclude")
            .description("Comma delimited list specifying which headers from the original message to exclude in the exported email message. Blank means don't exclude any headers.")
            .defaultValue("")
            .addValidator(Validator.VALID)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All messages that are the are successfully received from Email server and converted to FlowFiles are routed to this relationship")
            .build();

    final protected List<PropertyDescriptor> DESCRIPTORS;

    final protected Set<Relationship> RELATIONSHIPS;

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected volatile BlockingQueue<Message> messageQueue;

    protected volatile String displayUrl;

    protected volatile ProcessSession processSession;

    protected volatile boolean shouldSetDeleteFlag;

    protected volatile String folderName;

    public ConsumeEWS(){
        final Set<Relationship> relationshipSet = new HashSet<>();
        relationshipSet.add(REL_SUCCESS);

        RELATIONSHIPS = relationshipSet;

        final List<PropertyDescriptor> descriptors = new ArrayList<>();

        descriptors.add(USER);
        descriptors.add(PASSWORD);
        descriptors.add(FOLDER);
        descriptors.add(FETCH_SIZE);
        descriptors.add(SHOULD_DELETE_MESSAGES);
        descriptors.add(CONNECTION_TIMEOUT);
        descriptors.add(EXCHANGE_VERSION);
        descriptors.add(EWS_URL);
        descriptors.add(USE_AUTODISCOVER);
        descriptors.add(SHOULD_MARK_READ);
        descriptors.add(INCLUDE_EMAIL_HEADERS);
        descriptors.add(EXCLUDE_EMAIL_HEADERS);

        DESCRIPTORS = descriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession processSession) throws ProcessException {
        if(this.messageQueue == null){
            int fetchSize = context.getProperty(FETCH_SIZE).evaluateAttributeExpressions().asInteger();
            this.messageQueue = new ArrayBlockingQueue<>(fetchSize);
        }

        this.folderName = context.getProperty(FOLDER).getValue();

        Message emailMessage = this.receiveMessage(context);
        if (emailMessage != null) {
            this.transfer(emailMessage, context, processSession);
        } else {
            //No new messages found, yield the processor
            context.yield();
        }
    }

    protected ExchangeService initializeIfNecessary(ProcessContext context) throws ProcessException {
        ExchangeVersion ver = ExchangeVersion.valueOf(context.getProperty(EXCHANGE_VERSION).getValue());
        ExchangeService service = new ExchangeService(ver);

        final String timeoutInMillis = String.valueOf(context.getProperty(CONNECTION_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS));
        service.setTimeout(Integer.parseInt(timeoutInMillis));

        String userEmail = context.getProperty(USER).getValue();
        String password = context.getProperty(PASSWORD).getValue();

        ExchangeCredentials credentials = new WebCredentials(userEmail, password);
        service.setCredentials(credentials);

        Boolean useAutodiscover = context.getProperty(USE_AUTODISCOVER).asBoolean();
        if(useAutodiscover){
            try {
                service.autodiscoverUrl(userEmail, new RedirectionUrlCallback());
            } catch (Exception e) {
                throw new ProcessException("Failure setting Autodiscover URL from email address.", e);
            }
        } else {
            String ewsURL = context.getProperty(EWS_URL).getValue();
            try {
                service.setUrl(new URI(ewsURL));
            } catch (URISyntaxException e) {
                throw new ProcessException("Failure setting EWS URL.", e);
            }
        }

        return service;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Specifies the value for '" + propertyDescriptorName + "' Java Mail property.")
                .name(propertyDescriptorName).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).dynamic(true)
                .build();
    }

    /**
     * Return the target receivere's mail protocol (e.g., imap, pop etc.)
     */
    protected String getProtocol(ProcessContext processContext) {
        return "ews";
    }

    /**
     * Fills the internal message queue if such queue is empty. This is due to
     * the fact that per single session there may be multiple messages retrieved
     * from the email server (see FETCH_SIZE).
     */
    protected void fillMessageQueueIfNecessary(ProcessContext context) throws ProcessException {
        if (this.messageQueue.isEmpty()) {
            ExchangeService service = this.initializeIfNecessary(context);
            boolean deleteOnRead = context.getProperty(SHOULD_DELETE_MESSAGES).getValue().equals("true");
            boolean markAsRead = context.getProperty(SHOULD_MARK_READ).getValue().equals("true");
            String includeHeaders = context.getProperty(INCLUDE_EMAIL_HEADERS).getValue();
            String excludeHeaders = context.getProperty(EXCLUDE_EMAIL_HEADERS).getValue();

            List<String> includeHeadersList = null;
            List<String> excludeHeadersList = null;

            if (!StringUtils.isEmpty(includeHeaders)) {
                includeHeadersList = Arrays.asList(includeHeaders.split(","));
            }

            if (!StringUtils.isEmpty(excludeHeaders)) {
                excludeHeadersList = Arrays.asList(excludeHeaders.split(","));
            }

            try {
                //Get Folder
                Folder folder = getFolder(service);

                ItemView view = new ItemView(messageQueue.remainingCapacity());
                view.getOrderBy().add(ItemSchema.DateTimeReceived, SortDirection.Ascending);

                SearchFilter sf = new SearchFilter.SearchFilterCollection(LogicalOperator.And, new SearchFilter.IsEqualTo(EmailMessageSchema.IsRead, false));
                FindItemsResults<Item> findResults = service.findItems(folder.getId(), sf, view);

                if(findResults == null || findResults.getItems().size()== 0){
                    return;
                }

                service.loadPropertiesForItems(findResults, PropertySet.FirstClassProperties);

                for (Item item : findResults) {
                    EmailMessage ewsMessage = (EmailMessage) item;
                    messageQueue.add(parseMessage(ewsMessage,includeHeadersList,excludeHeadersList));

                    if(deleteOnRead){
                        ewsMessage.delete(DeleteMode.HardDelete);
                    } else if(markAsRead){
                        ewsMessage.setIsRead(true);
                        ewsMessage.update(ConflictResolutionMode.AlwaysOverwrite);
                    }
                }

                service.close();
            } catch (Exception e) {
                throw new ProcessException("Failed retrieving new messages from EWS.", e);
            }
        }
    }

    protected Folder getFolder(ExchangeService service) {
        Folder folder;
        if(folderName.equals("INBOX")){
            try {
                folder = Folder.bind(service, WellKnownFolderName.Inbox);
            } catch (Exception e) {
                throw new ProcessException("Failed to bind Inbox Folder on EWS Server", e);
            }
        } else {
            FolderView view = new FolderView(10);
            view.setTraversal(FolderTraversal.Deep);
            SearchFilter searchFilter = new SearchFilter.IsEqualTo(FolderSchema.DisplayName, folderName);
            try {
                FindFoldersResults foldersResults = service.findFolders(WellKnownFolderName.Root,searchFilter, view);
                ArrayList<Folder> folderIds = foldersResults.getFolders();
                if(folderIds.size() > 1){
                    throw new ProcessException("More than 1 folder found with the name " + folderName);
                }

                folder = Folder.bind(service, folderIds.get(0).getId());
            } catch (Exception e) {
                throw new ProcessException("Search for Inbox Subfolder failed.", e);
            }
        }
        return folder;
    }

    public MimeMessage parseMessage(EmailMessage item, List<String> hdrIncludeList, List<String> hdrExcludeList) throws Exception {
        EmailMessage ewsMessage = item;
        final String bodyText = ewsMessage.getBody().toString();

        MultiPartEmail mm;

        if(ewsMessage.getBody().getBodyType() == BodyType.HTML){
            mm = new HtmlEmail().setHtmlMsg(bodyText);
        } else {
            mm = new MultiPartEmail();
            mm.setMsg(bodyText);
        }
        mm.setHostName("NiFi-EWS");
        //from
        mm.setFrom(ewsMessage.getFrom().getAddress());
        //to recipients
        ewsMessage.getToRecipients().forEach(x->{
            try {
                mm.addTo(x.getAddress());
            } catch (EmailException e) {
                throw new ProcessException("Failed to add TO recipient.", e);
            }
        });
        //cc recipients
        ewsMessage.getCcRecipients().forEach(x->{
            try {
                mm.addCc(x.getAddress());
            } catch (EmailException e) {
                throw new ProcessException("Failed to add CC recipient.", e);
            }
        });
        //subject
        mm.setSubject(ewsMessage.getSubject());
        //sent date
        mm.setSentDate(ewsMessage.getDateTimeSent());
        //add message headers
        ewsMessage.getInternetMessageHeaders().getItems().stream()
                .filter(x -> (hdrIncludeList == null || hdrIncludeList.isEmpty() || hdrIncludeList.contains(x.getName()))
                        && (hdrExcludeList == null || hdrExcludeList.isEmpty() || !hdrExcludeList.contains(x.getName())))
                .forEach(x-> mm.addHeader(x.getName(), x.getValue()));

        //Any attachments
        if(ewsMessage.getHasAttachments()){
            ewsMessage.getAttachments().forEach(x->{
                try {
                    FileAttachment file = (FileAttachment)x;
                    file.load();

                    ByteArrayDataSource bds = new ByteArrayDataSource(file.getContent(), file.getContentType());

                    mm.attach(bds,file.getName(), "", EmailAttachment.ATTACHMENT);
                } catch (MessagingException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        mm.buildMimeMessage();
        return mm.getMimeMessage();
    }

    /**
     * Disposes the message by converting it to a {@link FlowFile} transferring
     * it to the REL_SUCCESS relationship.
     */
    private void transfer(Message emailMessage, ProcessContext context, ProcessSession processSession) {
        long start = System.nanoTime();
        FlowFile flowFile = processSession.create();

        flowFile = processSession.append(flowFile, new OutputStreamCallback() {
            @Override
            public void process(final OutputStream out) throws IOException {
                try {
                    emailMessage.writeTo(out);
                } catch (MessagingException e) {
                    throw new IOException(e);
                }
            }
        });

        long executionDuration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        String fromAddressesString = "";
        try {
            Address[] fromAddresses = emailMessage.getFrom();
            if (fromAddresses != null) {
                fromAddressesString = Arrays.asList(fromAddresses).toString();
            }
        } catch (MessagingException e) {
            this.logger.warn("Faild to retrieve 'From' attribute from Message.");
        }

        processSession.getProvenanceReporter().receive(flowFile, this.displayUrl, "Received message from " + fromAddressesString, executionDuration);
        this.getLogger().info("Successfully received {} from {} in {} millis", new Object[]{flowFile, fromAddressesString, executionDuration});
        processSession.transfer(flowFile, REL_SUCCESS);

        try {
            emailMessage.setFlag(Flags.Flag.DELETED, this.shouldSetDeleteFlag);
        } catch (MessagingException e) {
            this.logger.warn("Failed to set DELETE Flag on the message, data duplication may occur.");
        }
    }

    /**
     * Receives message from the internal queue filling up the queue if
     * necessary.
     */
    protected Message receiveMessage(ProcessContext context) {
        Message emailMessage = null;
        try {
            this.fillMessageQueueIfNecessary(context);
            emailMessage = this.messageQueue.poll(1, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            context.yield();
            this.logger.error("Failed retrieving messages from EWS.", e);
            Thread.currentThread().interrupt();
            this.logger.debug("Current thread is interrupted");
        }
        return emailMessage;
    }



    @OnStopped
    public void stop(ProcessContext processContext) {
        this.flushRemainingMessages(processContext);
    }

    /**
     * Will flush the remaining messages when this processor is stopped.
     */
    protected void flushRemainingMessages(ProcessContext processContext) {
        Message emailMessage;
        try {
            while ((emailMessage = this.messageQueue.poll(1, TimeUnit.MILLISECONDS)) != null) {
                this.transfer(emailMessage, processContext, this.processSession);
                this.processSession.commit();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            this.logger.debug("Current thread is interrupted");
        }
    }

    static class RedirectionUrlCallback implements IAutodiscoverRedirectionUrl {
        public boolean autodiscoverRedirectionUrlValidationCallback(
                String redirectionUrl) {
            return redirectionUrl.toLowerCase().startsWith("https://");
        }
    }
}
