package com.canva.sqs.local.filesystem;

import org.apache.http.annotation.ThreadSafe;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Single JMV + Single Host Exclusive lock based on JDK {@link ReentrantLock} for JVM locking
 * and {@link FileChannel} for host's file locking.
 * <p>
 * Main tool for implementation of critical sections for file based SQS
 * <p>
 * Implemented as AutoCloseable for convenient usage in try-with-resources
 * <p>
 * Usage example:
 * try (GlobalCloseableLock ignored = new GlobalCloseableLock(path).lock()) {
 * // critical section commands
 * }
 *
 * @author Alexander Pronin
 * @since 03/11/2017
 */
@SuppressWarnings("WeakerAccess")
@ThreadSafe
public class GlobalCloseableLock implements AutoCloseable {
    private static final Lock GLOBAL_LOCK = new ReentrantLock();
    private final FileChannel channel;
    private FileLock fileLock;

    public GlobalCloseableLock(String fileName) throws IOException {
        this.channel =
                FileChannel.open(Paths.get(fileName + ".lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    }

    @Override
    public void close() {
        try {
            if (fileLock != null) {
                fileLock.release();
            }
            channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        GLOBAL_LOCK.unlock();
        try {
            channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public GlobalCloseableLock lock() throws IOException {
        //noinspection LockAcquiredButNotSafelyReleased
        GLOBAL_LOCK.lock();
        fileLock = channel.lock();
        return this;
    }
}
