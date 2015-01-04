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
package org.apache.nifi.admin.service.action;

import java.util.List;
import java.util.Map;
import org.apache.nifi.admin.dao.DAOFactory;
import org.apache.nifi.admin.service.AccountNotFoundException;
import org.apache.nifi.admin.service.AdministrationException;
import org.apache.nifi.authorization.AuthorityProvider;
import org.apache.nifi.authorization.DownloadAuthorization;
import org.apache.nifi.authorization.exception.AuthorityAccessException;
import org.apache.nifi.authorization.exception.UnknownIdentityException;

/**
 * Attempts to obtain authorization to download the content with the specified
 * attributes for the specified user.
 */
public class AuthorizeDownloadAction implements AdministrationAction<DownloadAuthorization> {

    private final List<String> dnChain;
    private final Map<String, String> attributes;

    public AuthorizeDownloadAction(List<String> dnChain, Map<String, String> attributes) {
        this.dnChain = dnChain;
        this.attributes = attributes;
    }

    @Override
    public DownloadAuthorization execute(DAOFactory daoFactory, AuthorityProvider authorityProvider) {
        try {
            return authorityProvider.authorizeDownload(dnChain, attributes);
        } catch (UnknownIdentityException uie) {
            throw new AccountNotFoundException(uie.getMessage(), uie);
        } catch (AuthorityAccessException aae) {
            throw new AdministrationException(aae.getMessage(), aae);
        }
    }

}
