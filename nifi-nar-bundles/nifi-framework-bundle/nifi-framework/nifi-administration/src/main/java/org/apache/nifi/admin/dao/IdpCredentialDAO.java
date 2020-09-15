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
package org.apache.nifi.admin.dao;

import org.apache.nifi.idp.IdpCredential;

public interface IdpCredentialDAO {

    IdpCredential createCredential(IdpCredential credential) throws DataAccessException;

    IdpCredential findCredentialById(int id) throws DataAccessException;

    IdpCredential findCredentialByIdentity(String identity) throws DataAccessException;

    int deleteCredentialById(int id) throws DataAccessException;

    int deleteCredentialByIdentity(String identity) throws DataAccessException;

}
