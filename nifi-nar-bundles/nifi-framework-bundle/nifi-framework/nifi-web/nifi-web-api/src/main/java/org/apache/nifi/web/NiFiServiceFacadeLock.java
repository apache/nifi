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
package org.apache.nifi.web;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Aspect to limit access into the core.
 */
@Aspect
public class NiFiServiceFacadeLock {
    private static final Logger logger = LoggerFactory.getLogger(NiFiServiceFacadeLock.class);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    /* delegate methods through the wrapped view model */
    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
            + "execution(* create*(..))")
    public Object createLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        return proceedWithWriteLock(proceedingJoinPoint);
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
        + "execution(* clear*(..))")
    public Object clearLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        return proceedWithWriteLock(proceedingJoinPoint);
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
            + "execution(* delete*(..))")
    public Object deleteLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        return proceedWithWriteLock(proceedingJoinPoint);
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
            + "execution(* remove*(..))")
    public Object removeLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        return proceedWithWriteLock(proceedingJoinPoint);
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
            + "execution(* update*(..))")
    public Object updateLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        return proceedWithWriteLock(proceedingJoinPoint);
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
            + "execution(* set*(..))")
    public Object setLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        return proceedWithWriteLock(proceedingJoinPoint);
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
            + "execution(* copy*(..))")
    public Object copyLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        return proceedWithWriteLock(proceedingJoinPoint);
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
            + "execution(* import*(..))")
    public Object importLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        return proceedWithWriteLock(proceedingJoinPoint);
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
            + "execution(* export*(..))")
    public Object exportLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        return proceedWithWriteLock(proceedingJoinPoint);
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
            + "execution(* submit*(..))")
    public Object submitLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        return proceedWithWriteLock(proceedingJoinPoint);
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
            + "execution(* schedule*(..))")
    public Object scheduleLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        return proceedWithWriteLock(proceedingJoinPoint);
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
        + "execution(* activate*(..))")
    public Object activateLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        return proceedWithWriteLock(proceedingJoinPoint);
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
        + "execution(* populate*(..))")
    public Object populateLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        return proceedWithWriteLock(proceedingJoinPoint);
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
            + "execution(* get*(..))")
    public Object getLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        return proceedWithReadLock(proceedingJoinPoint);
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
            + "execution(* is*(..))")
    public Object isLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        return proceedWithReadLock(proceedingJoinPoint);
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
            + "execution(* search*(..))")
    public Object searchLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        return proceedWithReadLock(proceedingJoinPoint);
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
        + "execution(* verify*(..))")
    public Object verifyLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        return proceedWithReadLock(proceedingJoinPoint);
    }


    private Object proceedWithReadLock(final ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        final long beforeLock = System.nanoTime();
        long afterLock = 0L;

        readLock.lock();
        try {
            afterLock = System.nanoTime();
            return proceedingJoinPoint.proceed();
        } finally {
            readLock.unlock();

            final long afterProcedure = System.nanoTime();
            final String procedure = proceedingJoinPoint.getSignature().toLongString();
            logger.debug("In order to perform procedure {}, it took {} nanos to obtain the Read Lock {} and {} nanos to invoke the method",
                procedure, afterLock - beforeLock, readLock, afterProcedure - afterLock);
        }
    }

    private Object proceedWithWriteLock(final ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        final long beforeLock = System.nanoTime();
        long afterLock = 0L;

        writeLock.lock();
        try {
            afterLock = System.nanoTime();
            return proceedingJoinPoint.proceed();
        } finally {
            writeLock.unlock();

            final long afterProcedure = System.nanoTime();
            final String procedure = proceedingJoinPoint.getSignature().toLongString();
            logger.debug("In order to perform procedure {}, it took {} nanos to obtain the Write Lock {} and {} nanos to invoke the method",
                procedure, afterLock - beforeLock, writeLock, afterProcedure - afterLock);
        }
    }

}
