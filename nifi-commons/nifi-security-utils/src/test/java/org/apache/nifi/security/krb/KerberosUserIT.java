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
package org.apache.nifi.security.krb;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class KerberosUserIT {

    @ClassRule
    public static TemporaryFolder tmpDir = new TemporaryFolder();

    private static KDCServer kdc;

    private static KerberosPrincipal principal1;
    private static File principal1KeytabFile;

    private static KerberosPrincipal principal2;
    private static File principal2KeytabFile;

    private static KerberosPrincipal principal3;
    private static final String principal3Password = "changeme";

    @BeforeClass
    public static void setupClass() throws Exception {
        kdc = new KDCServer(tmpDir.newFolder("mini-kdc_"));
        kdc.setMaxTicketLifetime("15"); // set ticket lifetime to 15 seconds so we can test relogin
        kdc.start();

        principal1 = new KerberosPrincipal("user1@" + kdc.getRealm());
        principal1KeytabFile = tmpDir.newFile("user1.keytab");
        kdc.createKeytabPrincipal(principal1KeytabFile, "user1");

        principal2 = new KerberosPrincipal("user2@" + kdc.getRealm());
        principal2KeytabFile = tmpDir.newFile("user2.keytab");
        kdc.createKeytabPrincipal(principal2KeytabFile, "user2");

        principal3 = new KerberosPrincipal("user3@" + kdc.getRealm());
        kdc.createPasswordPrincipal("user3", principal3Password);
    }

    @Test
    public void testKeytabUserSuccessfulLoginAndLogout() throws LoginException {
        // perform login for user1
        final KerberosUser user1 = new KerberosKeytabUser(principal1.getName(), principal1KeytabFile.getAbsolutePath());
        user1.login();

        // perform login for user2
        final KerberosUser user2 = new KerberosKeytabUser(principal2.getName(), principal2KeytabFile.getAbsolutePath());
        user2.login();

        // verify user1 Subject only has user1 principal
        final Subject user1Subject = ((KerberosKeytabUser) user1).getSubject();
        final Set<Principal> user1SubjectPrincipals = user1Subject.getPrincipals();
        assertEquals(1, user1SubjectPrincipals.size());
        assertEquals(principal1.getName(), user1SubjectPrincipals.iterator().next().getName());

        // verify user2 Subject only has user2 principal
        final Subject user2Subject = ((KerberosKeytabUser) user2).getSubject();
        final Set<Principal> user2SubjectPrincipals = user2Subject.getPrincipals();
        assertEquals(1, user2SubjectPrincipals.size());
        assertEquals(principal2.getName(), user2SubjectPrincipals.iterator().next().getName());

        // call check/relogin and verify neither user performed a relogin
        assertFalse(user1.checkTGTAndRelogin());
        assertFalse(user2.checkTGTAndRelogin());

        // perform logout for both users
        user1.logout();
        user2.logout();

        // verify subjects have no more principals
        assertEquals(0, user1Subject.getPrincipals().size());
        assertEquals(0, user2Subject.getPrincipals().size());
    }

    @Test
    public void testKeytabLoginWithUnknownPrincipal() throws LoginException {
        final String unknownPrincipal = "doesnotexist@" + kdc.getRealm();
        final KerberosUser user1 = new KerberosKeytabUser(unknownPrincipal, principal1KeytabFile.getAbsolutePath());
        try {
            user1.login();
            fail("Login should have failed");
        } catch (Exception e) {
            // exception is expected here
            //e.printStackTrace();
        }
    }

    @Test
    public void testPasswordUserSuccessfulLoginAndLogout() throws LoginException {
        // perform login for user
        final KerberosUser user = new KerberosPasswordUser(principal3.getName(), principal3Password);
        user.login();

        // verify user Subject only has user principal
        final Subject userSubject = ((KerberosPasswordUser) user).getSubject();
        final Set<Principal> userSubjectPrincipals = userSubject.getPrincipals();
        assertEquals(1, userSubjectPrincipals.size());
        assertEquals(principal3.getName(), userSubjectPrincipals.iterator().next().getName());

        // call check/relogin and verify neither user performed a relogin
        assertFalse(user.checkTGTAndRelogin());

        // perform logout for both users
        user.logout();

        // verify subjects have no more principals
        assertEquals(0, userSubject.getPrincipals().size());
    }

    @Test(expected = LoginException.class)
    public void testPasswordUserLoginWithInvalidPassword() throws LoginException {
        // perform login for user
        final KerberosUser user = new KerberosPasswordUser("user3", "NOT THE PASSWORD");
        user.login();
    }

    @Test
    public void testCheckTGTAndRelogin() throws LoginException, InterruptedException {
        final KerberosUser user1 = new KerberosKeytabUser(principal1.getName(), principal1KeytabFile.getAbsolutePath());
        user1.login();

        // Since we set the lifetime to 15 seconds we should hit a relogin before 15 attempts

        boolean performedRelogin = false;
        for (int i=0; i < 30; i++) {
            Thread.sleep(1000);
            System.out.println("checkTGTAndRelogin #" + i);
            performedRelogin = user1.checkTGTAndRelogin();

            if (performedRelogin) {
                System.out.println("Performed relogin!");
                break;
            }
        }
        assertEquals(true, performedRelogin);
    }

    @Test
    public void testKeytabAction() {
        final KerberosUser user1 = new KerberosKeytabUser(principal1.getName(), principal1KeytabFile.getAbsolutePath());

        final AtomicReference<String> resultHolder = new AtomicReference<>(null);
        final PrivilegedAction privilegedAction = () -> {
            resultHolder.set("SUCCESS");
            return null;
        };

        final ProcessContext context = Mockito.mock(ProcessContext.class);
        final ComponentLog logger = Mockito.mock(ComponentLog.class);

        // create the action to test and execute it
        final KerberosAction kerberosAction = new KerberosAction(user1, privilegedAction, context, logger);
        kerberosAction.execute();

        // if the result holder has the string success then we know the action executed
        assertEquals("SUCCESS", resultHolder.get());
    }

}
