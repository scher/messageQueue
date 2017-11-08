package com.canva.sqs.local.filesystem;

import org.apache.http.annotation.ThreadSafe;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

import static java.nio.file.StandardOpenOption.*;

/**
 * File based implementation "semaphore" abstraction.
 *
 * @see GlobalCloseableLock
 * @author Alexander Pronin
 * @since 07/11/2017
 */
@SuppressWarnings("WeakerAccess")
@ThreadSafe
public class SemaphoreFile {

    private SemaphoreFile() {
    }

    /**
     * Atomically increases counter
     *
     * @param path path to semaphore file
     */
    public static void increaseCounter(String path) {
        try (GlobalCloseableLock ignored = new GlobalCloseableLock(path).lock()) {
            String value =
                    Files.readAllLines(Paths.get(path)).stream()
                            .findFirst()
                            .map(s -> Integer.valueOf(s) + 1)
                            .orElse(1)
                            .toString();
            Files.write(Paths.get(path), Collections.singleton(value), WRITE, TRUNCATE_EXISTING);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Atomically decreases counter
     * @param path path to semaphore file
     */
    public static void decreaseCounter(String path) {
        try (GlobalCloseableLock ignored = new GlobalCloseableLock(path).lock()) {
            String value =
                    Files.readAllLines(Paths.get(path)).stream()
                            .findFirst()
                            .map(s -> Integer.valueOf(s) - 1)
                            .orElse(0)
                            .toString();
            Files.write(Paths.get(path), Collections.singleton(value), CREATE, WRITE, TRUNCATE_EXISTING);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Atomically checks current counter and executes action if counter equals to zero
     *
     * @param path    path to semaphore file
     * @param execute runnable to execute in current thread
     * @return true if action was executed
     */
    public static boolean executeIfZero(String path, Runnable execute) {
        boolean result = false;
        try (GlobalCloseableLock ignored = new GlobalCloseableLock(path)) {
            String value = Files.readAllLines(Paths.get(path)).stream().findFirst().orElse("0");
            if (Integer.valueOf(value).equals(0)) {
                execute.run();
                result = true;
            }
            Files.write(Paths.get(path), Collections.singleton(value), CREATE, WRITE, TRUNCATE_EXISTING);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
