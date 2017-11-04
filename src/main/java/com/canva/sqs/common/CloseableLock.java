package com.canva.sqs.common;

import java.io.Closeable;
import java.util.concurrent.locks.Lock;

/**
 * @author Alexander Pronin
 * @since 03/11/2017
 */
public class CloseableLock implements AutoCloseable {
    private final Lock lock;

    public CloseableLock(Lock lock) {
        this.lock = lock;
    }

    @Override
    public void close() {
        lock.unlock();
    }

    public CloseableLock lock() {
        lock.lock();
        return this;
    }
}
