package com.canva.sqs.local.filesystem;

import com.amazonaws.services.sqs.model.Message;
import org.apache.http.annotation.ThreadSafe;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.nio.file.StandardOpenOption.*;
import static java.util.stream.Collectors.toList;

/**
 * Stateless utility class for atomic writing and reading for file.
 *
 * @see GlobalCloseableLock
 * @author Alexander Pronin
 * @since 06/11/2017
 */
@SuppressWarnings("WeakerAccess")
@ThreadSafe
public class SynchronizedFileReaderWriter {
    private SynchronizedFileReaderWriter() {
    }

    public static List<Message> removeMessagesFromFile(Path filePath,
                                                       Function<List<String>, Map<Boolean, List<Message>>> messageExtractor) {
        List<Message> result = new ArrayList<>();
        try (GlobalCloseableLock ignored = new GlobalCloseableLock(filePath.toString()).lock()) {
            List<String> messages = Files.readAllLines(filePath);

            Map<Boolean, List<Message>> splitted = messageExtractor.apply(messages);
            result.addAll(splitted.get(Boolean.TRUE));
            List<String> messagesToFile = splitted.get(Boolean.FALSE)
                    .stream()
                    .map(MessageRecord::fromMessageToString)
                    .collect(toList());

            Files.write(filePath, messagesToFile, WRITE, TRUNCATE_EXISTING);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void addMessageToEndOfFile(List<Message> messages, Path filePath) {
        try (GlobalCloseableLock ignored = new GlobalCloseableLock(filePath.toString()).lock()) {
            List<String> toWrite = messages.stream().map(MessageRecord::fromMessageToString).collect(toList());
            Files.write(filePath, toWrite, APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void addMessagesToBeginningOfFile(List<Message> messages, Path filePath) {
        try (GlobalCloseableLock ignored = new GlobalCloseableLock(filePath.toString()).lock()) {
            List<String> oldMessages = Files.readAllLines(filePath);
            List<String> toWrite = messages.stream().map(MessageRecord::fromMessageToString).collect(toList());
            toWrite.addAll(oldMessages);
            Files.write(filePath, toWrite, WRITE, TRUNCATE_EXISTING);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
