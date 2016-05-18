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

package org.apache.nifi.web.revision;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.web.FlowModification;
import org.apache.nifi.web.InvalidRevisionException;
import org.apache.nifi.web.Revision;
import org.junit.Assert;
import org.junit.Test;

public class TestNaiveRevisionManager {
    private static final String CLIENT_1 = "client-1";
    private static final String COMPONENT_1 = "component-1";

    private RevisionUpdate<Object> components(final Revision revision) {
        return new StandardRevisionUpdate<Object>(null, new FlowModification(revision, null));
    }

    private RevisionUpdate<Object> components(final Revision revision, final Revision... additionalRevisions) {
        final Set<Revision> revisionSet = new HashSet<>();
        for (final Revision rev : additionalRevisions) {
            revisionSet.add(rev);
        }
        return components(revision, revisionSet);
    }

    private RevisionUpdate<Object> components(final Revision revision, final Set<Revision> additionalRevisions) {
        final Set<RevisionUpdate<Object>> components = new HashSet<>();
        for (final Revision rev : additionalRevisions) {
            components.add(new StandardRevisionUpdate<Object>(null, new FlowModification(rev, null)));
        }
        return new StandardRevisionUpdate<Object>(null, new FlowModification(revision, null), additionalRevisions);
    }

    @Test
    public void testTypicalFlow() throws ExpiredRevisionClaimException {
        final RevisionManager revisionManager = new NaiveRevisionManager();
        final Revision originalRevision = new Revision(0L, CLIENT_1, COMPONENT_1);
        final RevisionClaim claim = revisionManager.requestClaim(originalRevision);
        assertNotNull(claim);

        revisionManager.updateRevision(claim, "unit test", () -> components(new Revision(1L, CLIENT_1, COMPONENT_1)));

        final Revision updatedRevision = revisionManager.getRevision(originalRevision.getComponentId());
        assertNotNull(updatedRevision);
        assertEquals(originalRevision.getClientId(), updatedRevision.getClientId());
        assertEquals(originalRevision.getComponentId(), updatedRevision.getComponentId());
        assertEquals(1L, updatedRevision.getVersion().longValue());
    }

    @Test
    public void testExpiration() throws InterruptedException {
        final RevisionManager revisionManager = new NaiveRevisionManager(10, TimeUnit.MILLISECONDS);
        final Revision originalRevision = new Revision(0L, CLIENT_1, COMPONENT_1);
        final RevisionClaim claim = revisionManager.requestClaim(originalRevision);
        assertNotNull(claim);

        Thread.sleep(100);

        try {
            revisionManager.updateRevision(claim, "unit test", () -> components(originalRevision, claim.getRevisions()));
            Assert.fail("Expected Revision Claim to have expired but it did not");
        } catch (final ExpiredRevisionClaimException erce) {
            // expected
        }
    }

    @Test(timeout = 15000)
    public void testConflictingClaimsFromDifferentClients() {
        final RevisionManager revisionManager = new NaiveRevisionManager(2, TimeUnit.SECONDS);
        final Revision originalRevision = new Revision(0L, CLIENT_1, COMPONENT_1);
        final RevisionClaim claim = revisionManager.requestClaim(originalRevision);
        assertNotNull(claim);

        final Revision differentClientRevision = new Revision(0L, "client-2", COMPONENT_1);
        final long start = System.nanoTime();
        final RevisionClaim differentClientClaim = revisionManager.requestClaim(differentClientRevision);
        final long nanos = System.nanoTime() - start;

        // we should block for 2 seconds. But the timing won't necessarily be exact,
        // so we ensure that it takes at least 1.5 seconds to provide a little wiggle room.
        final long minExpectedNanos = TimeUnit.MILLISECONDS.toNanos(1500);
        assertTrue(nanos > minExpectedNanos);

        // We should not get a Revision Claim because the revision is already claimed by a different client id
        assertNotNull(differentClientClaim);
        final Set<Revision> newRevisions = differentClientClaim.getRevisions();
        assertEquals(1, newRevisions.size());
        assertEquals(differentClientRevision, newRevisions.iterator().next());
    }

    @Test
    public void testGetWithReadLockNoContention() {
        final RevisionManager revisionManager = new NaiveRevisionManager(3, TimeUnit.SECONDS);
        final Object returnedValue = revisionManager.get(COMPONENT_1, revision -> revision);
        assertTrue(returnedValue instanceof Revision);

        final Revision revision = (Revision) returnedValue;
        assertEquals(0L, revision.getVersion().longValue());
        assertNull(revision.getClientId());
        assertEquals(COMPONENT_1, revision.getComponentId());
    }

    @Test(timeout = 10000)
    public void testGetWithReadLockAndContentionWithTimeout() {
        final RevisionManager revisionManager = new NaiveRevisionManager(2, TimeUnit.SECONDS);
        final Revision originalRevision = new Revision(8L, CLIENT_1, COMPONENT_1);
        final RevisionClaim claim = revisionManager.requestClaim(originalRevision);
        assertNotNull(claim);

        final long start = System.nanoTime();
        final Object returnValue = new Object();
        final Object valueReturned = revisionManager.get(COMPONENT_1, revision -> returnValue);
        final long nanos = System.nanoTime() - start;

        final long minExpectedNanos = TimeUnit.MILLISECONDS.toNanos(1500L);
        assertTrue(nanos > minExpectedNanos);
        assertEquals(returnValue, valueReturned);
    }

    @Test(timeout = 10000)
    public void testGetWithReadLockAndContentionWithEventualLockResolution() {
        final RevisionManager revisionManager = new NaiveRevisionManager(10, TimeUnit.MINUTES);
        final Revision originalRevision = new Revision(8L, CLIENT_1, COMPONENT_1);
        final RevisionClaim claim = revisionManager.requestClaim(originalRevision);
        assertNotNull(claim);

        final Revision updatedRevision = new Revision(100L, CLIENT_1, COMPONENT_1);

        // Create a thread that will hold the lock for 2 seconds and then will return an updated revision
        final Thread updateRevisionThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    revisionManager.updateRevision(claim, "unit test", () -> {
                        // Wait 2 seconds and then return
                        try {
                            Thread.sleep(2000L);
                        } catch (Exception e) {
                        }

                        return components(updatedRevision);
                    });
                } catch (ExpiredRevisionClaimException e) {
                    Assert.fail("Revision expired unexpectedly");
                }
            }
        });
        updateRevisionThread.start();

        final long start = System.nanoTime();
        final Object returnValue = new Object();
        final Object valueReturned = revisionManager.get(COMPONENT_1, revision -> {
            Assert.assertEquals(updatedRevision, revision);
            return returnValue;
        });
        final long nanos = System.nanoTime() - start;

        final long minExpectedNanos = TimeUnit.MILLISECONDS.toNanos(1500L);
        assertTrue(nanos > minExpectedNanos);
        assertEquals(returnValue, valueReturned);
    }

    @Test(timeout = 10000)
    public void testDeleteRevision() {
        final RevisionManager revisionManager = new NaiveRevisionManager(2, TimeUnit.MINUTES);
        final Revision firstRevision = new Revision(1L, CLIENT_1, COMPONENT_1);
        final RevisionClaim firstClaim = revisionManager.requestClaim(firstRevision);
        assertNotNull(firstClaim);

        final Revision secondRevision = new Revision(2L, CLIENT_1, COMPONENT_1);
        final FlowModification mod = new FlowModification(secondRevision, "unit test");
        revisionManager.updateRevision(firstClaim, "unit test", () -> new StandardRevisionUpdate<Void>(null, mod, null));

        final Revision updatedRevision = revisionManager.getRevision(COMPONENT_1);
        assertEquals(secondRevision, updatedRevision);

        final RevisionClaim secondClaim = revisionManager.requestClaim(updatedRevision);
        assertNotNull(secondClaim);

        final Object obj = new Object();
        final Object ret = revisionManager.deleteRevision(secondClaim, () -> obj);
        assertEquals(obj, ret);

        final Revision curRevision = revisionManager.getRevision(COMPONENT_1);
        assertNotNull(curRevision);
        assertEquals(0L, curRevision.getVersion().longValue());
        assertNull(curRevision.getClientId());
        assertEquals(COMPONENT_1, curRevision.getComponentId());
    }


    @Test(timeout = 10000)
    public void testSameClientDifferentRevisionsDoNotBlockEachOther() {
        final RevisionManager revisionManager = new NaiveRevisionManager(2, TimeUnit.MINUTES);
        final Revision firstRevision = new Revision(1L, CLIENT_1, COMPONENT_1);
        final RevisionClaim firstClaim = revisionManager.requestClaim(firstRevision);
        assertNotNull(firstClaim);

        final Revision secondRevision = new Revision(1L, CLIENT_1, "component-2");
        final RevisionClaim secondClaim = revisionManager.requestClaim(secondRevision);
        assertNotNull(secondClaim);
    }

    @Test(timeout = 10000)
    public void testSameClientSameRevisionBlocks() throws InterruptedException, ExecutionException {
        final RevisionManager revisionManager = new NaiveRevisionManager(2, TimeUnit.MINUTES);
        final Revision firstRevision = new Revision(1L, CLIENT_1, COMPONENT_1);
        final RevisionClaim firstClaim = revisionManager.requestClaim(firstRevision);
        assertNotNull(firstClaim);

        final Revision secondRevision = new Revision(1L, CLIENT_1, COMPONENT_1);
        final Runnable runnable = new Runnable() {
            @Override
            public void run() {
                revisionManager.requestClaim(secondRevision);
            }
        };
        final ExecutorService exec = Executors.newFixedThreadPool(1);
        final Future<?> future = exec.submit(runnable);

        try {
            future.get(2, TimeUnit.SECONDS);
            Assert.fail("Call to obtain claim on revision did not block when claim was already held");
        } catch (TimeoutException e) {
            // Expected
        }
    }

    @Test(timeout = 10000)
    public void testDifferentClientDifferentRevisionsDoNotBlockEachOther() {
        final RevisionManager revisionManager = new NaiveRevisionManager(2, TimeUnit.MINUTES);
        final Revision firstRevision = new Revision(1L, CLIENT_1, COMPONENT_1);
        final RevisionClaim firstClaim = revisionManager.requestClaim(firstRevision);
        assertNotNull(firstClaim);

        final Revision secondRevision = new Revision(1L, "client-2", "component-2");
        final RevisionClaim secondClaim = revisionManager.requestClaim(secondRevision);
        assertNotNull(secondClaim);
    }

    @Test(timeout = 10000)
    public void testDifferentOrderedRevisionsDoNotCauseDeadlock() throws ExpiredRevisionClaimException, InterruptedException {
        // Because we block before obtaining a claim on a revision if another client has the revision claimed,
        // we should not have an issue if Client 1 requests a claim on revisions 'a' and 'b' while Client 2
        // requests a claim on revisions 'b' and 'c' and Client 3 requests a claim on revisions 'c' and 'a'.
        final RevisionManager revisionManager = new NaiveRevisionManager(2, TimeUnit.MINUTES);
        final Revision revision1a = new Revision(1L, "client-1", "a");
        final Revision revision1b = new Revision(1L, "client-1", "b");

        final Revision revision2b = new Revision(2L, "client-2", "b");
        final Revision revision2c = new Revision(2L, "client-2", "c");

        final Revision revision3c = new Revision(3L, "client-3", "c");
        final Revision revision3a = new Revision(3L, "client-3", "a");

        final RevisionClaim claim1 = revisionManager.requestClaim(Arrays.asList(revision1a, revision1b));
        assertNotNull(claim1);

        final AtomicBoolean claim2Obtained = new AtomicBoolean(false);
        final AtomicBoolean claim3Obtained = new AtomicBoolean(false);

        final AtomicReference<RevisionClaim> claim2Ref = new AtomicReference<>();
        final AtomicReference<RevisionClaim> claim3Ref = new AtomicReference<>();
        new Thread(new Runnable() {
            @Override
            public void run() {
                final RevisionClaim claim2 = revisionManager.requestClaim(Arrays.asList(revision2b, revision2c));
                assertNotNull(claim2);
                claim2Obtained.set(true);
                claim2Ref.set(claim2);

                try {
                    revisionManager.updateRevision(claim2, "unit test", () -> components(new Revision(3L, "client-2", "b"), new Revision(3L, "client-2", "c")));
                } catch (ExpiredRevisionClaimException e) {
                    Assert.fail("Revision unexpected expired");
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                final RevisionClaim claim3 = revisionManager.requestClaim(Arrays.asList(revision3c, revision3a));
                assertNotNull(claim3);
                claim3Obtained.set(true);
                claim3Ref.set(claim3);

                try {
                    revisionManager.updateRevision(claim3Ref.get(), "unit test", () -> components(new Revision(2L, "client-3", "c"), new Revision(2L, "client-3", "a")));
                } catch (ExpiredRevisionClaimException e) {
                    Assert.fail("Revision unexpected expired");
                }
            }
        }).start();

        Thread.sleep(250L);

        assertFalse(claim2Obtained.get());
        assertFalse(claim3Obtained.get());
        revisionManager.updateRevision(claim1, "unit test", () -> components(new Revision(3L, "client-1", "a"), new Revision(2L, "client-1", "b")));

        Thread.sleep(250L);
        assertTrue(claim2Obtained.get() && claim3Obtained.get());

        assertEquals(2L, revisionManager.getRevision("a").getVersion().longValue());

        // The version for 'c' could be either 2 or 3, depending on which request completed first.
        final long versionC = revisionManager.getRevision("c").getVersion().longValue();
        assertTrue(versionC == 2 || versionC == 3);

        assertEquals(3L, revisionManager.getRevision("b").getVersion().longValue());
    }

    @Test(timeout = 10000)
    public void testReleaseClaim() {
        final RevisionManager revisionManager = new NaiveRevisionManager(10, TimeUnit.MINUTES);
        final Revision firstRevision = new Revision(1L, CLIENT_1, COMPONENT_1);
        final RevisionClaim claim = revisionManager.requestClaim(firstRevision);
        assertNotNull(claim);

        final RevisionClaim invalidClaim = new StandardRevisionClaim(new Revision(2L, "client-2", COMPONENT_1));
        assertFalse(revisionManager.releaseClaim(invalidClaim));

        assertTrue(revisionManager.releaseClaim(claim));
    }

    @Test(timeout = 10000)
    public void testCancelClaimSameThread() {
        final RevisionManager revisionManager = new NaiveRevisionManager(10, TimeUnit.MINUTES);
        final Revision firstRevision = new Revision(1L, CLIENT_1, COMPONENT_1);
        final RevisionClaim claim = revisionManager.requestClaim(firstRevision);
        assertNotNull(claim);

        assertFalse(revisionManager.cancelClaim("component-2"));
        assertTrue(revisionManager.cancelClaim(COMPONENT_1));
    }

    @Test(timeout = 10000)
    public void testCancelClaimDifferentThread() throws InterruptedException {
        final RevisionManager revisionManager = new NaiveRevisionManager(10, TimeUnit.MINUTES);
        final Revision firstRevision = new Revision(1L, CLIENT_1, COMPONENT_1);
        final RevisionClaim claim = revisionManager.requestClaim(firstRevision);
        assertNotNull(claim);

        final Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                assertFalse(revisionManager.cancelClaim("component-2"));
                assertFalse(revisionManager.cancelClaim(COMPONENT_1));
            }
        });
        t.setDaemon(true);
        t.start();

        Thread.sleep(1000L);
        assertTrue(revisionManager.cancelClaim(COMPONENT_1));
    }

    @Test(timeout = 10000)
    public void testUpdateWithSomeWrongRevision() {
        final RevisionManager revisionManager = new NaiveRevisionManager(10, TimeUnit.MINUTES);
        final Revision component1V1 = new Revision(1L, CLIENT_1, COMPONENT_1);
        final Revision component2V1 = new Revision(1L, CLIENT_1, "component-2");
        final RevisionClaim claim = revisionManager.requestClaim(Arrays.asList(component1V1, component2V1));
        assertNotNull(claim);

        // Perform update but only update the revision for component-2
        final Revision component1V2 = new Revision(2L, "client-2", COMPONENT_1);
        revisionManager.updateRevision(claim, "unit test", new UpdateRevisionTask<Void>() {
            @Override
            public RevisionUpdate<Void> update() {
                return new StandardRevisionUpdate<>(null, new FlowModification(component1V2, "unit test"));
            }
        });

        // Obtain a claim with correct revisions
        final Revision component2V2 = new Revision(2L, "client-2", "component-2");
        revisionManager.requestClaim(Arrays.asList(component1V2, component2V1));

        // Attempt to update with incorrect revision for second component
        final RevisionClaim wrongClaim = new StandardRevisionClaim(component1V2, component2V2);

        final Revision component1V3 = new Revision(3L, CLIENT_1, COMPONENT_1);
        try {
            revisionManager.updateRevision(wrongClaim, "unit test",
                () -> new StandardRevisionUpdate<>(null, new FlowModification(component1V3, "unit test"), Collections.emptySet()));
            Assert.fail("Expected an Invalid Revision Exception");
        } catch (final InvalidRevisionException ire) {
            // expected
        }

        // release claim should fail because we are passing the wrong revision for component 2
        assertFalse(revisionManager.releaseClaim(new StandardRevisionClaim(component1V2, component2V2)));

        // release claim should succeed because we are now using the proper revisions
        assertTrue(revisionManager.releaseClaim(new StandardRevisionClaim(component1V2, component2V1)));

        // verify that we can update again.
        final RevisionClaim thirdClaim = revisionManager.requestClaim(Arrays.asList(component1V2, component2V1));
        assertNotNull(thirdClaim);
        revisionManager.updateRevision(thirdClaim, "unit test", () -> new StandardRevisionUpdate<>(null, new FlowModification(component1V3, "unit test")));
    }
}
