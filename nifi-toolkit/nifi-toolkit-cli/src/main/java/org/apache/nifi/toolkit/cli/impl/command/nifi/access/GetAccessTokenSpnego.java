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
package org.apache.nifi.toolkit.cli.impl.command.nifi.access;

import org.apache.commons.cli.MissingOptionException;
import org.apache.nifi.documentation.init.NopComponentLog;
import org.apache.nifi.security.krb.KerberosAction;
import org.apache.nifi.security.krb.KerberosKeytabUser;
import org.apache.nifi.security.krb.KerberosPasswordUser;
import org.apache.nifi.security.krb.KerberosTicketCacheUser;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.StringResult;
import org.apache.nifi.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class GetAccessTokenSpnego extends AbstractNiFiCommand<StringResult> {

    public GetAccessTokenSpnego() {
        super("get-access-token-spnego", StringResult.class);
    }

    @Override
    public String getDescription() {
        return "Authenticates to NiFi via SPNEGO and returns an access token for use " +
                "on future requests as the value of the " + CommandOption.BEARER_TOKEN.getLongName() + " argument. " +
                "If a keytab or password is not specified, then the ticket cache will be used and it is " +
                "assumed that a kinit was done for the given principal outside of the CLI.";
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.KERBEROS_PRINCIPAL.createOption());
        addOption(CommandOption.KERBEROS_KEYTAB.createOption());
        addOption(CommandOption.KERBEROS_PASSWORD.createOption());
    }

    @Override
    public StringResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {

        final String principal = getRequiredArg(properties, CommandOption.KERBEROS_PRINCIPAL);

        final String keytab = getArg(properties, CommandOption.KERBEROS_KEYTAB);
        final String password = getArg(properties, CommandOption.KERBEROS_PASSWORD);

        if (!StringUtils.isBlank(keytab) && !StringUtils.isBlank(password)) {
            throw new MissingOptionException("Only one of keytab or password can be specified");
        }

        final KerberosUser kerberosUser;
        if (!StringUtils.isBlank(keytab)) {
            final File keytabFile = new File(keytab);
            if (!keytabFile.exists()) {
                throw new CommandException("Unable to find keytab file at: " + keytabFile.getAbsolutePath());
            }
            kerberosUser = new KerberosKeytabUser(principal, keytab);
        } else if (!StringUtils.isBlank(password)) {
            kerberosUser = new KerberosPasswordUser(principal, password);
        } else {
            kerberosUser = new KerberosTicketCacheUser(principal);
        }

        final KerberosAction<String> kerberosAction = new KerberosAction<>(
                kerberosUser,
                () -> client.getAccessClient().getTokenFromKerberosTicket(),
                new NopComponentLog());

        final String token = kerberosAction.execute();
        return new StringResult(token, getContext().isInteractive());
    }
}
