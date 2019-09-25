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

package org.apache.nifi.processors.kudu;

import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import javax.security.auth.login.LoginException;
import org.apache.kudu.client.KuduClient;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.security.krb.KerberosUser;

public abstract class AbstractMockKuduProcessor extends ScanKudu {

  private boolean loggedIn = false;
  private boolean loggedOut = false;

  @Override
  protected KerberosUser loginKerberosUser(final String principal, final String keytab) throws LoginException {
    return new KerberosUser() {

      @Override
      public void login() {
        loggedIn = true;
      }

      @Override
      public void logout() {
        loggedOut = true;
      }

      @Override
      public <T> T doAs(final PrivilegedAction<T> action) throws IllegalStateException {
        return action.run();
      }

      @Override
      public <T> T doAs(final PrivilegedExceptionAction<T> action) throws IllegalStateException, PrivilegedActionException {
        try {
          return action.run();
        } catch (Exception e) {
          throw new PrivilegedActionException(e);
        }
      }

      @Override
      public boolean checkTGTAndRelogin() {
        return true;
      }

      @Override
      public boolean isLoggedIn() {
        return loggedIn && !loggedOut;
      }

      @Override
      public String getPrincipal() {
        return principal;
      }
    };
  }

  public boolean loggedIn() {
    return loggedIn;
  }

  public boolean loggedOut() {
    return loggedOut;
  }

  @Override
  public KuduClient buildClient(final String masters, ProcessContext context) {
    return this.kuduClient;
  }

  @Override
  public KuduClient getKuduClient() {
    return this.kuduClient;
  }

}
