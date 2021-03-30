package org.apache.nifi.reporting;

import java.util.concurrent.atomic.AtomicLong;

public class BulletinIdProvider {

    private static final AtomicLong currentId = new AtomicLong(0);

    public static long getUniqueId() {
        return currentId.getAndIncrement();
    }

    private BulletinIdProvider() {
    }
}
