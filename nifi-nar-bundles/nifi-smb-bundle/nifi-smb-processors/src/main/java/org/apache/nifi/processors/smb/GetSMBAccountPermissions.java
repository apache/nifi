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
package org.apache.nifi.processors.smb;

import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.rapid7.client.dcerpc.dto.SID;
import com.rapid7.client.dcerpc.mslsad.LocalSecurityAuthorityService;
import com.rapid7.client.dcerpc.mslsad.dto.PolicyHandle;
import com.rapid7.client.dcerpc.transport.RPCTransport;
import com.rapid7.client.dcerpc.transport.SMBTransportFactories;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.apache.nifi.smb.common.SmbUtils.buildSmbClient;
@Tags({"windows, smb, security, account, permissions"})
@CapabilityDescription("Retrieves account permissions for a given account")
@WritesAttributes({
@WritesAttribute(attribute = "accountRights", description = "The account rights of the given account")
})
public class GetSMBAccountPermissions extends AbstractProcessor  {

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Hostname")
            .description("The hostname of the SMB server.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DOMAIN = new PropertyDescriptor.Builder()
            .name("Domain")
            .description("The domain used for authentication. Optional, in most cases username and password is sufficient.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("The username used for authentication. If no username is set then anonymous authentication is attempted.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("The password used for authentication. Required if Username is set.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();
    
    public static final PropertyDescriptor ACCOUNT_SID = new PropertyDescriptor.Builder()
            .name("Account SID")
            .description("SID of account for which to get permissions")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final List<PropertyDescriptor> properties = List.of(ACCOUNT_SID, HOSTNAME, DOMAIN, USERNAME, PASSWORD);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles are routed to success relationship when account rights are retrieved")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed to failure relationship when account rights cannot be retrieved")
            .build();

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.unmodifiableSet(
                new LinkedHashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        SMBClient smbClient = initSmbClient(context);
        final String hostname = context.getProperty(HOSTNAME).getValue();
        final String domain = context.getProperty(DOMAIN).getValue();
        final String username = context.getProperty(USERNAME).getValue();
        final String password = context.getProperty(PASSWORD).getValue();

        AuthenticationContext ac = null;
        if (username != null && password != null) {
            ac = new AuthenticationContext(
                    username,
                    password.toCharArray(),
                    domain);
        } else {
            ac = AuthenticationContext.anonymous();
        }
        FlowFile flowFile = session.get();
        if(flowFile == null) {
            flowFile = session.create();
        }
        try (Connection connection = smbClient.connect(hostname);
            Session smbSession = connection.authenticate(ac)) {
            final RPCTransport transport = SMBTransportFactories.SRVSVC.getTransport(smbSession);
            final LocalSecurityAuthorityService service = new LocalSecurityAuthorityService(transport);
            final SID accountSid = SID.fromString(context.getProperty(ACCOUNT_SID).evaluateAttributeExpressions().getValue());
            PolicyHandle handle = service.openPolicyHandle(hostname, LocalSecurityAuthorityService.MAXIMUM_ALLOWED);
            String[] accountRights = service.getAccountRights(handle, accountSid);

            // add account rights to incoming flowfile and pass it to success relationship
            if (flowFile != null) {
                flowFile = session.putAttribute(flowFile, "accountRights", String.join(",", accountRights));
                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch (Exception e) {
            getLogger().error("Could not establish smb connection because of error {}", new Object[]{e});
            context.yield();
            smbClient.getServerList().unregister(hostname);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private SMBClient initSmbClient(final ProcessContext context) {
        return buildSmbClient(context);
    }
}
