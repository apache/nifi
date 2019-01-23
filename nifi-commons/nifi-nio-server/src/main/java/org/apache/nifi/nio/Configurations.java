package org.apache.nifi.nio;

public class Configurations {
    private final int ioThreadPoolSize;
    private final long ioTimeoutMillis;
    private final long idleConnectionTimeoutMillis;
    private final int ioBufferSize;

    private Configurations(int ioThreadPoolSize, long ioTimeoutMillis, long idleConnectionTimeoutMillis, int ioBufferSize) {
        this.ioThreadPoolSize = ioThreadPoolSize;
        this.ioTimeoutMillis = ioTimeoutMillis;
        this.idleConnectionTimeoutMillis = idleConnectionTimeoutMillis;
        this.ioBufferSize = ioBufferSize;
    }

    public int getIoThreadPoolSize() {
        return ioThreadPoolSize;
    }

    public long getIoTimeoutMillis() {
        return ioTimeoutMillis;
    }

    public long getIdleConnectionTimeoutMillis() {
        return idleConnectionTimeoutMillis;
    }

    public int getIoBufferSize() {
        return ioBufferSize;
    }

    public static class Builder {
        private int ioThreadPoolSize = 10;
        private long ioTimeoutMillis = 30_000;
        private long idleConnectionTimeoutMillis = 60_000;
        private int ioBufferSize = 16 * 1024;

        public Builder ioThreadPoolSize(int ioThreadPoolSize) {
            this.ioThreadPoolSize = ioThreadPoolSize;
            return this;
        }

        /**
         * Specify network communication or thread synchronization timeout.
         */
        public Builder ioTimeout(long ioTimeoutMillis) {
            this.ioTimeoutMillis = ioTimeoutMillis;
            return this;
        }

        public Builder idleConnectionTimeoutMillis(long timeout) {
            this.idleConnectionTimeoutMillis = timeout;
            return this;
        }

        public Builder ioBufferSize(int ioBufferSize) {
            this.ioBufferSize = ioBufferSize;
            return this;
        }

        public Configurations build() {
            return new Configurations(ioThreadPoolSize, ioTimeoutMillis, idleConnectionTimeoutMillis, ioBufferSize);
        }
    }
}
