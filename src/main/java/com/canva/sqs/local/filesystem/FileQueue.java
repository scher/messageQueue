package com.canva.sqs.local.filesystem;

import com.amazonaws.services.sqs.model.Message;
import com.canva.sqs.local.IdsGenerator;
import com.canva.sqs.local.Queue;
import org.apache.http.annotation.ThreadSafe;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.canva.sqs.local.filesystem.FileDescriptor.*;
import static com.canva.sqs.local.filesystem.SynchronizedFileReaderWriter.*;
import static java.util.stream.Collectors.*;

/**
 * Durable ThreadSafe and single-host-safe file based queue implementation.
 * <p>
 * Suitable for single-host usage.
 * <p>
 * Queue size is not limited
 * This implementation leverages {@link GlobalCloseableLock} to achieve exclusive locking.
 * <p>
 * Actually queue implementation is stateless. Singleton pattern used for dependency injection in {@link com.canva.sqs.local.AbstractLocalQueue}.
 * <p>
 * Uses "lazy" invalidation of inflight message.
 * Queue tries to invalidate messages on each {@link #receiveMessage(String)} request.
 *
 * @author Alexander Pronin
 * @see GlobalCloseableLock
 * @see com.canva.sqs.local.AbstractLocalQueue
 * @since 04/11/2017
 */
@ThreadSafe
public class FileQueue implements Queue {
    private static final String SQS_QUEUES_DIR_KEY = "sqs.queues.dir";

    private static final Function<List<String>, Map<Boolean, List<Message>>> FIRST_MESSAGE_EXTRACTOR =
            messageRecords -> {
                final int[] i = {1};
                return messageRecords.stream()
                        .map(MessageRecord::fromStringToMessage)
                        .collect(partitioningBy(m -> i[0]++ == 1));
            };

    private static final Function<String, Function<List<String>, Map<Boolean, List<Message>>>>
            BY_RECEIPT_HANDLER_SPLITTER =
            s -> messageRecords -> messageRecords.stream()
                    .map(MessageRecord::fromStringToMessage)
                    .collect(partitioningBy(m -> m.getReceiptHandle().equals(s)));

    private static final BiFunction<Long, Long, Function<List<String>, Map<Boolean, List<Message>>>>
            BY_INFLIGHT_DELAY_SPLITTER =
            (inflightDelay, curTime) -> messageRecords -> messageRecords.stream().map(MessageRecord::fromString)
                    .collect(partitioningBy(mr -> curTime - mr.getInflightSince() > inflightDelay,
                            mapping(MessageRecord::toMessage, toList())));

    private static final FileQueue INSTANCE = new FileQueue();
    private static Properties properties;

    @SuppressWarnings("WeakerAccess")
    public static Queue getInstance() {
        return INSTANCE;
    }

    public static void setProperties(Properties properties) {
        FileQueue.properties = properties;
    }

    private long getInflightDelay() {
        return Long.parseLong(properties.getProperty(INFLIGHT_TIMEOUT_SECONDS_KEY));
    }

    private IdsGenerator getIdsGenerator(String queueDir) {
        return new FileIdsGenerator(IDS_CONFIG.getPath(queueDir));
    }

    private FileQueue() {
    }

    @Override
    @Nonnull
    public String sendMessage(String queueUrl, String messageBody) {
        String messageId = getIdsGenerator(queueUrl).generateMessageId();
        Message message = new Message().withBody(messageBody).withMessageId(String.valueOf(messageId));
        addMessageToEndOfFile(Collections.singletonList(message), MESSAGES.getPath(queueUrl));
        return messageId;
    }

    @Override
    public Optional<Message> receiveMessage(String queueUrl) {
        invalidateInflight(queueUrl);
        return removeMessagesFromFile(MESSAGES.getPath(queueUrl), FIRST_MESSAGE_EXTRACTOR)
                .stream()
                .findFirst()
                .map(message -> {
                    String receiptHandle = getIdsGenerator(queueUrl).generateRecipientHandlerId(message);
                    message.withReceiptHandle(receiptHandle);
                    addMessageToEndOfFile(Collections.singletonList(message), INFLIGHT.getPath(queueUrl));
                    return message;
                });
    }

    private void invalidateInflight(String queueUrl) {
        long curTime = System.currentTimeMillis();
        addMessagesToBeginningOfFile(
                removeMessagesFromFile(INFLIGHT.getPath(queueUrl),
                        BY_INFLIGHT_DELAY_SPLITTER.apply(getInflightDelay(), curTime)),
                MESSAGES.getPath(queueUrl)
        );
    }

    @Override
    public void invalidateNow(String queueUrl, String receiptHandle) {
        addMessagesToBeginningOfFile(
                removeMessagesFromFile(
                        INFLIGHT.getPath(queueUrl),
                        BY_RECEIPT_HANDLER_SPLITTER.apply(receiptHandle)
                ),
                MESSAGES.getPath(queueUrl)
        );
    }

    @Override
    public void deleteMessage(String queueUrl, String receiptHandle) {
        removeMessagesFromFile(INFLIGHT.getPath(queueUrl), BY_RECEIPT_HANDLER_SPLITTER.apply(receiptHandle));
    }

    @Override
    public void cleanup(String queueUrl) {
        try {
            Path rootPath = Paths.get(queueUrl);
            Files.walk(rootPath, FileVisitOption.FOLLOW_LINKS)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .peek(System.out::println)
                    .forEach(File::delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("WeakerAccess")
    public static void init(String queueName) {
        String queuesBaseDirStr = properties.getProperty(SQS_QUEUES_DIR_KEY);

        Path queueUrl = Paths.get(queuesBaseDirStr, queueName);

        try (GlobalCloseableLock ignored = new GlobalCloseableLock(queuesBaseDirStr).lock()) {
            if (!Files.exists(queueUrl)) {
                Files.createDirectory(queueUrl);
                Files.createFile(IDS_CONFIG.getPath(queueUrl.toString()));
                Files.createFile(MESSAGES.getPath(queueUrl.toString()));
                Files.createFile(INFLIGHT.getPath(queueUrl.toString()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
