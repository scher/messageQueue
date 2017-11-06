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

    public static void main(String[] args) {
//        String fileName = "/tmp/queueLock";
//        FileChannel channel = FileChannel.open(Paths.get(fileName), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
//
//        try (GlobalCloseableLock l = new GlobalCloseableLock(channel).lock()){
//            System.out.println("HeyHey");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

//        Lock osLock = new ReentrantLock();
//
//        String fileName = "/tmp/queueLock";
//        FileChannel channel = FileChannel.channel(Paths.get(fileName), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
////        FileLock lock = channel.lock();
////        lock.
//
//        new Thread(() -> {
//            FileLock lock;
////            osLock.lock();
//            try {
//                lock = channel.lock();
//                System.out.println("Locked by " + Thread.currentThread().getName());
//                LockSupport.parkNanos(10_000_000);
//                lock.release();
//                System.out.println("UNLocked by " + Thread.currentThread().getName());
//
//            } catch (IOException e) {
//                e.printStackTrace();
//            } finally {
////                osLock.unlock();
//            }
//
//        }).start();
//        LockSupport.parkNanos(1_000_000);
//
//        new Thread(() -> {
////            osLock.lock();
//            try {
//                channel.lock();
//            } catch (IOException e) {
//                e.printStackTrace();
//            } finally {
////                osLock.unlock();
//            }
//            System.out.println("Locked by " + Thread.currentThread().getName());
//        }).start();
//
////        System.out.println("Locked!");
//        while (true) {
//            LockSupport.parkNanos(10_000_000);
//        }
    }
}
