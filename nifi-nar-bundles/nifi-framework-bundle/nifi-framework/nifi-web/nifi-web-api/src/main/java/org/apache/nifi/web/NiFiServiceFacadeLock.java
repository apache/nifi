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

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Aspect to limit access into the core.
 */
@Aspect
public class NiFiServiceFacadeLock {

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    /* delegate methods through the wrapped view model */
    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
            + "execution(* create*(..))")
    public Object createLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        writeLock.lock();
        try {
            return proceedingJoinPoint.proceed();
        } finally {
            writeLock.unlock();
        }
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
        + "execution(* clear*(..))")
    public Object clearLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        writeLock.lock();
        try {
            return proceedingJoinPoint.proceed();
        } finally {
            writeLock.unlock();
        }
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
            + "execution(* delete*(..))")
    public Object deleteLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        writeLock.lock();
        try {
            return proceedingJoinPoint.proceed();
        } finally {
            writeLock.unlock();
        }
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
            + "execution(* remove*(..))")
    public Object removeLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        writeLock.lock();
        try {
            return proceedingJoinPoint.proceed();
        } finally {
            writeLock.unlock();
        }
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
            + "execution(* update*(..))")
    public Object updateLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        writeLock.lock();
        try {
            return proceedingJoinPoint.proceed();
        } finally {
            writeLock.unlock();
        }
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
            + "execution(* set*(..))")
    public Object setLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        writeLock.lock();
        try {
            return proceedingJoinPoint.proceed();
        } finally {
            writeLock.unlock();
        }
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
            + "execution(* copy*(..))")
    public Object copyLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        writeLock.lock();
        try {
            return proceedingJoinPoint.proceed();
        } finally {
            writeLock.unlock();
        }
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
            + "execution(* import*(..))")
    public Object importLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        writeLock.lock();
        try {
            return proceedingJoinPoint.proceed();
        } finally {
            writeLock.unlock();
        }
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
            + "execution(* export*(..))")
    public Object exportLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        writeLock.lock();
        try {
            return proceedingJoinPoint.proceed();
        } finally {
            writeLock.unlock();
        }
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
            + "execution(* submit*(..))")
    public Object submitLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        writeLock.lock();
        try {
            return proceedingJoinPoint.proceed();
        } finally {
            writeLock.unlock();
        }
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
            + "execution(* schedule*(..))")
    public Object scheduleLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        writeLock.lock();
        try {
            return proceedingJoinPoint.proceed();
        } finally {
            writeLock.unlock();
        }
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
            + "execution(* get*(..))")
    public Object getLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        readLock.lock();
        try {
            return proceedingJoinPoint.proceed();
        } finally {
            readLock.unlock();
        }
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
            + "execution(* is*(..))")
    public Object isLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        readLock.lock();
        try {
            return proceedingJoinPoint.proceed();
        } finally {
            readLock.unlock();
        }
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
            + "execution(* search*(..))")
    public Object searchLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        readLock.lock();
        try {
            return proceedingJoinPoint.proceed();
        } finally {
            readLock.unlock();
        }
    }

    @Around("within(org.apache.nifi.web.NiFiServiceFacade+) && "
            + "execution(* verify*(..))")
    public Object verifyLock(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        readLock.lock();
        try {
            return proceedingJoinPoint.proceed();
        } finally {
            readLock.unlock();
        }
    }
}
