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
package org.apache.nifi.authentication.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InvalidObjectException;
import java.util.List;
import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.ValidationEvent;
import javax.xml.bind.ValidationEventHandler;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.nifi.authentication.file.generated.ObjectFactory;
import org.apache.nifi.authentication.file.generated.UserCredentials;
import org.apache.nifi.authentication.file.generated.UserCredentialsList;

import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;


/**
 * Data access for a simple local XML credentials file.  The credentials file
 * contains usernames and password hashes in bcrypt format.  Any compatible
 * bcrypt "2a" implementation may be used to populate the credentials file,
 * or you may use the {@link CredentialsCLI} class in this package as an admin tool.
 *
 * @see CredentialsCLI
 */
public class CredentialsStore {

    private static final String CREDENTIALS_XSD = "/credentials.xsd";
    private static final String JAXB_GENERATED_PATH = "org.apache.nifi.authentication.file.generated";
    private static final JAXBContext JAXB_CONTEXT = initializeJaxbContext();
    private static final ObjectFactory factory = new ObjectFactory();

    private PasswordEncoder passwordEncoder = new BCryptPasswordEncoder();
    private File credentialsFile;
    private long credentialsListLastModified;
    private UserCredentialsList credentialsList = factory.createUserCredentialsList();

    private static JAXBContext initializeJaxbContext() {
        try {
            return JAXBContext.newInstance(JAXB_GENERATED_PATH,  CredentialsStore.class.getClassLoader());
        } catch (JAXBException e) {
            throw new RuntimeException("Failed creating JAXBContext for " + CredentialsStore.class.getCanonicalName());
        }
    }

    private static ValidationEventHandler defaultValidationEventHandler = new ValidationEventHandler() {
        @Override
        public boolean handleEvent(ValidationEvent event) {
            return false;
        }
    };

    static UserCredentialsList loadCredentialsList(String filePath) throws Exception {
        final File credentialsFile = new File(filePath);
        return loadCredentialsList(credentialsFile, defaultValidationEventHandler);
    }

    static UserCredentialsList loadCredentialsList(File credentialsFile, ValidationEventHandler validationEventHandler) throws Exception {
        if (credentialsFile.exists()) {
            final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            final Schema schema = schemaFactory.newSchema(UserCredentialsList.class.getResource(CREDENTIALS_XSD));

            final Unmarshaller unmarshaller = JAXB_CONTEXT.createUnmarshaller();
            unmarshaller.setSchema(schema);
            unmarshaller.setEventHandler(validationEventHandler);
            final JAXBElement<UserCredentialsList> element = unmarshaller.unmarshal(new StreamSource(credentialsFile),
                    UserCredentialsList.class);
            UserCredentialsList credentialsList = element.getValue();
            return credentialsList;
        } else {
            final String notFoundMessage = "The credentials configuration file was not found at: " +
                    credentialsFile.getAbsolutePath();
            throw new FileNotFoundException(notFoundMessage);
        }
    }

    static void saveCredentialsList(UserCredentialsList credentialsList, File saveFile) throws Exception {
        final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        final Schema schema = schemaFactory.newSchema(UserCredentialsList.class.getResource(CREDENTIALS_XSD));
        final Marshaller marshaller = JAXB_CONTEXT.createMarshaller();
        final JAXBElement<UserCredentialsList> jaxbCredentialsList = factory.createCredentials(credentialsList);
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        marshaller.marshal(jaxbCredentialsList, saveFile);
    }

    public static CredentialsStore fromFile(String filePath) throws Exception {
        UserCredentialsList credentialsList = loadCredentialsList(filePath);
        CredentialsStore credStore = new CredentialsStore(credentialsList);
        return credStore;
    }

    public static CredentialsStore fromFile(File file) throws Exception {
        CredentialsStore credStore = new CredentialsStore(file);
        credStore.load();
        return credStore;
    }

    public CredentialsStore() {
    }

    public CredentialsStore(File credentialsFile) {
        this.credentialsFile = credentialsFile;
    }

    public CredentialsStore(UserCredentialsList credentialsList) {
        this.credentialsList = credentialsList;
    }

    UserCredentialsList getCredentialsList() {
        return credentialsList;
    }

    public void load() throws Exception {
        long credentialsFileLastModified = credentialsFile.lastModified();
        credentialsList = loadCredentialsList(credentialsFile, defaultValidationEventHandler);
        credentialsListLastModified = credentialsFileLastModified;
    }

    public boolean reloadIfModified() throws Exception {
        long credentialsFileLastModified = credentialsFile.lastModified();
        if (credentialsFileLastModified > credentialsListLastModified) {
            load();
            return true;
        }
        return false;
    }

    public UserCredentials addUser(String userName, String rawPassword) {
        UserCredentials userCreds = factory.createUserCredentials();
        userCreds.setName(userName);
        setPassword(userCreds, rawPassword);
        List<UserCredentials> usersList = credentialsList.getUser();
        usersList.add(userCreds);
        return userCreds;
    }

    public UserCredentials findUser(String userName) {
        List<UserCredentials> usersList = credentialsList.getUser();
        for (UserCredentials userCreds : usersList) {
            String credsUserName = userCreds.getName();
            if (userName.equalsIgnoreCase(credsUserName)) {
                return userCreds;
            }
        }
        return null;
    }

    UserCredentials setPassword(UserCredentials userCreds, String rawPassword) {
        BCryptPasswordEncoder encoder = new BCryptPasswordEncoder();
        String hashedPassword = encoder.encode(rawPassword);
        userCreds.setPasswordHash(hashedPassword);
        return userCreds;
    }

    public UserCredentials resetPassword(String userName, String rawPassword) {
        UserCredentials userCreds = findUser(userName);
        if (userCreds != null) {
            setPassword(userCreds, rawPassword);
        }
        return userCreds;
    }

    public boolean checkPassword(String userName, String rawPassword) {
        BCryptPasswordEncoder encoder = new BCryptPasswordEncoder();
        UserCredentials userCreds = findUser(userName);
        if (userCreds != null) {
            final String hashedPassword = userCreds.getPasswordHash();
            if (encoder.matches(rawPassword, hashedPassword)){
                return true;
            }
        }
        return false;
    }

    public boolean removeUser(String userName) {
        UserCredentials userCreds = findUser(userName);
        if (userCreds != null) {
            UserCredentialsList credentialsList = getCredentialsList();
            List<UserCredentials> usersList = credentialsList.getUser();
            boolean removed = usersList.remove(userCreds);
            return removed;
        }
        return false;
    }

    public void save() throws Exception {
        if (credentialsFile == null) {
            throw new InvalidObjectException("Credentials file has not been specified");
        }
        CredentialsStore.saveCredentialsList(credentialsList, credentialsFile);
        credentialsListLastModified = credentialsFile.lastModified();
    }

    public void save(File saveFile) throws Exception {
        credentialsFile = saveFile;
        save();
    }

    public void save(String saveFilePath) throws Exception {
        File saveFile = new File(saveFilePath);
        credentialsFile = saveFile;
        save();
    }
}
