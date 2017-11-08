package com.canva.sqs.local.filesystem;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

import static java.nio.file.StandardOpenOption.*;

/**
 * @author Alexander Pronin
 * @since 07/11/2017
 */
public class SemaphoreFile {

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

    public static void executeIfZero(String path, Runnable execute) {
        try (GlobalCloseableLock ignored = new GlobalCloseableLock(path)) {
            String value = Files.readAllLines(Paths.get(path)).stream().findFirst().orElse("0");
            if (Integer.valueOf(value).equals(0)) {
                execute.run();
            }
            Files.write(Paths.get(path), Collections.singleton(value), CREATE, WRITE, TRUNCATE_EXISTING);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
