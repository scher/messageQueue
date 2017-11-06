package com.canva.sqs.local.filesystem;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Alexander Pronin
 * @since 03/11/2017
 */
public class GlobalCloseableLock implements AutoCloseable {
    private static final Lock GLOBAL_LOCK = new ReentrantLock();
    private final FileChannel channel;
    private FileLock fileLock;

    public GlobalCloseableLock(FileChannel channel) {
        this.channel = channel;
    }

    @Override
    public void close() {
        try {
            if (fileLock != null) {
                fileLock.release();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        GLOBAL_LOCK.unlock();
    }

    public GlobalCloseableLock lock() throws IOException {
        GLOBAL_LOCK.lock();
        fileLock = channel.lock();
        return this;
    }
}
