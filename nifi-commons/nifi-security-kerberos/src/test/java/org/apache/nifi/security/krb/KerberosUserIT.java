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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import java.io.File;
import java.nio.file.Path;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KerberosUserIT {
    private static final Logger logger = LoggerFactory.getLogger(KerberosUserIT.class);

    private static KDCServer kdc;

    private static KerberosPrincipal principal1;
    private static File principal1KeytabFile;

    private static KerberosPrincipal principal2;
    private static File principal2KeytabFile;

    private static KerberosPrincipal principal3;
    private static final String principal3Password = "changeme";

    @BeforeAll
    public static void setupClass(@TempDir Path tmpDir) throws Exception {
        File kdcFolder = tmpDir.resolve("mini-kdc_").toFile();
        kdcFolder.mkdirs();

        kdc = new KDCServer(kdcFolder);
        kdc.setMaxTicketLifetime("15"); // set ticket lifetime to 15 seconds so we can test relogin
        kdc.start();

        principal1 = new KerberosPrincipal("user1@" + kdc.getRealm());
        principal1KeytabFile = tmpDir.resolve("user1.keytab").toFile();
        kdc.createKeytabPrincipal(principal1KeytabFile, "user1");

        principal2 = new KerberosPrincipal("user2@" + kdc.getRealm());
        principal2KeytabFile = tmpDir.resolve("user2.keytab").toFile();
        kdc.createKeytabPrincipal(principal2KeytabFile, "user2");

        principal3 = new KerberosPrincipal("user3@" + kdc.getRealm());
        kdc.createPasswordPrincipal("user3", principal3Password);
    }

    @Test
    public void testKeytabUserSuccessfulLoginAndLogout() {
        // perform login for user1
        final KerberosKeytabUser user1 = new KerberosKeytabUser(principal1.getName(), principal1KeytabFile.getAbsolutePath());
        user1.login();

        // perform login for user2
        final KerberosKeytabUser user2 = new KerberosKeytabUser(principal2.getName(), principal2KeytabFile.getAbsolutePath());
        user2.login();

        // verify user1 Subject only has user1 principal
        final Subject user1Subject = user1.getSubject();
        final Set<Principal> user1SubjectPrincipals = user1Subject.getPrincipals();
        assertEquals(1, user1SubjectPrincipals.size());
        assertEquals(principal1.getName(), user1SubjectPrincipals.iterator().next().getName());

        // verify user2 Subject only has user2 principal
        final Subject user2Subject = user2.getSubject();
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
    public void testKeytabLoginWithUnknownPrincipal() {
        final String unknownPrincipal = "doesnotexist@" + kdc.getRealm();
        final KerberosUser user1 = new KerberosKeytabUser(unknownPrincipal, principal1KeytabFile.getAbsolutePath());
        assertThrows(Exception.class, user1::login);
    }

    @Test
    public void testPasswordUserSuccessfulLoginAndLogout() {
        // perform login for user
        final KerberosPasswordUser user = new KerberosPasswordUser(principal3.getName(), principal3Password);
        user.login();

        // verify user Subject only has user principal
        final Subject userSubject = user.getSubject();
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

    @Test
    public void testPasswordUserLoginWithInvalidPassword() {
        // perform login for user
        final KerberosUser user = new KerberosPasswordUser("user3", "NOT THE PASSWORD");
        assertThrows(KerberosLoginException.class, user::login);
    }

    @Test
    public void testCheckTGTAndRelogin() throws InterruptedException {
        final KerberosUser user1 = new KerberosKeytabUser(principal1.getName(), principal1KeytabFile.getAbsolutePath());
        user1.login();

        // Since we set the lifetime to 15 seconds we should hit a relogin before 15 attempts

        boolean performedRelogin = false;
        for (int i = 0; i < 30; i++) {
            Thread.sleep(1000);
            logger.info("checkTGTAndRelogin #{}", i);
            performedRelogin = user1.checkTGTAndRelogin();

            if (performedRelogin) {
                logger.info("Performed relogin!");
                break;
            }
        }
        assertTrue(performedRelogin);

        Subject subject = user1.doAs((PrivilegedAction<Subject>) Subject::current);

        // verify only a single KerberosTicket exists in the Subject after relogin
        Set<KerberosTicket> kerberosTickets = subject.getPrivateCredentials(KerberosTicket.class);
        assertEquals(1, kerberosTickets.size());

        // verify the new ticket lifetime is valid for the current time
        KerberosTicket kerberosTicket = kerberosTickets.iterator().next();
        long currentTimeMillis = System.currentTimeMillis();
        long startMilli = kerberosTicket.getStartTime().toInstant().toEpochMilli();
        long endMilli = kerberosTicket.getEndTime().toInstant().toEpochMilli();
        logger.info("New ticket is valid for {}", TimeUnit.MILLISECONDS.toSeconds(endMilli - startMilli) + " seconds");
        assertTrue(startMilli < currentTimeMillis);
        assertTrue(endMilli > currentTimeMillis);
    }

    @Test
    public void testKeytabAction() {
        final KerberosUser user1 = new KerberosKeytabUser(principal1.getName(), principal1KeytabFile.getAbsolutePath());

        final AtomicReference<String> resultHolder = new AtomicReference<>(null);
        final PrivilegedExceptionAction<Void> privilegedAction = () -> {
            resultHolder.set("SUCCESS");
            return null;
        };

        final ComponentLog logger = Mockito.mock(ComponentLog.class);

        // create the action to test and execute it
        final KerberosAction<Void> kerberosAction = new KerberosAction<>(user1, privilegedAction, logger);
        kerberosAction.execute();

        // if the result holder has the string success then we know the action executed
        assertEquals("SUCCESS", resultHolder.get());
    }

}
