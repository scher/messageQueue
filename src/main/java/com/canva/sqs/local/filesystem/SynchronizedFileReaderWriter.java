package com.canva.sqs.local.filesystem;

import com.amazonaws.services.sqs.model.Message;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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
                                                       Function<List<String>, Map<Boolean, List<Message>>> messageExtractor,
                                                       Long time) {
        List<Message> result = new ArrayList<>();
        try (GlobalCloseableLock ignored = new GlobalCloseableLock(filePath.toString()).lock()) {
            List<String> messages = Files.readAllLines(filePath);

            Map<Boolean, List<Message>> splitted = messageExtractor.apply(messages);

            result.addAll(splitted.get(Boolean.TRUE));
            List<String> messagesToFile = splitted.get(Boolean.FALSE)
                    .stream()
                    .map(m -> MessageRecord.fromMessageToString(m, time))
                    .collect(toList());

            Files.write(filePath, messagesToFile, WRITE, TRUNCATE_EXISTING);
        } catch (IOException e) {
            e.printStackTrace();
        }

        fillBody(result, filePath);
        return result;
    }

    private static void fillBody(List<Message> result, Path filePath) {
        result.stream().forEach(message -> {
            try {
                String body = new String(Files.readAllBytes(getBodyFileName(filePath, message.getMessageId())));
                message.withBody(body);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static void addMessageToEndOfFile(List<Message> messages, Path filePath, Long time) {
        try (GlobalCloseableLock ignored = new GlobalCloseableLock(filePath.toString()).lock()) {
            messages.stream().forEach(message -> writeBody(filePath, message));

            List<String> toWrite = messages.stream()
                    .map(m -> MessageRecord.fromMessageToString(m, time))
                    .collect(toList());

            Files.write(filePath, toWrite, APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void writeBody(Path filePath, Message message) {
        try {
            Files.write(getBodyFileName(filePath, message.getMessageId()),
                    message.getBody().getBytes(), StandardOpenOption.CREATE);
        } catch (IOException e) {
            // log.error("some message, e)
            throw new RuntimeException(e);
        }
    }

    private static Path getBodyFileName(Path filePath, String messageId) {
        return Paths.get(filePath.toString() + "." + messageId);
    }

    public static void addMessagesToBeginningOfFile(List<Message> messages, Path filePath, Long time) {
        try (GlobalCloseableLock ignored = new GlobalCloseableLock(filePath.toString()).lock()) {
            List<String> oldMessages = Files.readAllLines(filePath);
            List<String> toWrite = messages.stream()
                    .map(m -> MessageRecord.fromMessageToString(m, time))
                    .collect(toList());
            toWrite.addAll(oldMessages);
            Files.write(filePath, toWrite, WRITE, TRUNCATE_EXISTING);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
