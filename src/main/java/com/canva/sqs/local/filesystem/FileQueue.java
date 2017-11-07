package com.canva.sqs.local.filesystem;

import com.amazonaws.services.sqs.model.Message;
import com.canva.sqs.SQSRunner;
import com.canva.sqs.local.IdsGenerator;
import com.canva.sqs.local.Queue;

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

import static com.canva.sqs.local.filesystem.SynchronizedFileReaderWriter.*;
import static java.util.stream.Collectors.*;

/**
 * Queue size is not limited
 *
 * @author Alexander Pronin
 * @since 04/11/2017
 */
public class FileQueue implements Queue {
    private static final String INFLIGHT_TIMEOUT_SECONDS_KEY = "sqs.inflight.timeout";
    private static final String SQS_QUEUES_DIR_KEY = "sqs.queues.dir";
    private static final String MESSAGES_FILE = "messages";
    private static final String INFLIGHT_FILE = "inflight";
    private static final String CONFIG_DIR = "config";
    private static final String IDS_CONFIG_FILE = "ids";

    private static final Function<List<String>, Map<Boolean, List<Message>>> FIRST_MESSAGE_EXTRACTOR =
            strings -> {
                final int[] i = {1};
                return strings.stream()
                        .map(MessageRecord::fromStringToMessage)
                        .collect(partitioningBy(m -> i[0]++ == 1));
            };

    private static final Function<String, Function<List<String>, Map<Boolean, List<Message>>>>
            BY_RECEIPT_HANDLER_SPLITTER =
            s -> strings -> strings.stream()
                    .map(MessageRecord::fromStringToMessage)
                    .collect(partitioningBy(m -> m.getReceiptHandle().equals(s)));

    private static final BiFunction<Long, Long, Function<List<String>, Map<Boolean, List<Message>>>>
            BY_INFLIGHT_DELAY_SPLITTER =
            (inflightDelay, curTime) -> strings -> strings.stream().map(MessageRecord::fromString)
                    .collect(partitioningBy(mr -> curTime - mr.getInflightSince() > inflightDelay,
                            mapping(MessageRecord::toMessage, toList())));

    private static final FileQueue INSTANCE = new FileQueue();

    @SuppressWarnings("WeakerAccess")
    public static Queue getInstance() {
        return INSTANCE;
    }

    private long getInflightDelay() {
        return Long.parseLong(SQSRunner.getInstance().getProperty(INFLIGHT_TIMEOUT_SECONDS_KEY));
    }

    private IdsGenerator getIdsGenerator(String queueDir) {
        return new FileIdsGenerator(getIdsGeneratorConfigFile(queueDir));
    }

    private static Path getConfigDir(String queueDir) {
        return Paths.get(queueDir, CONFIG_DIR);
    }

    private Path getIdsGeneratorConfigFile(String queueDir) {
        return Paths.get(getConfigDir(queueDir).toString(), IDS_CONFIG_FILE);
    }

    private static Path getInflightFile(String queueDir) {
        return Paths.get(queueDir, INFLIGHT_FILE);
    }

    private static Path getMessagesFile(String queueDir) {
        return Paths.get(queueDir, MESSAGES_FILE);
    }

    private FileQueue() {
    }

    //done
    @Override
    @Nonnull
    public String sendMessage(String queueUrl, String messageBody) {
        invalidateInflight(queueUrl);
        String messageId = getIdsGenerator(queueUrl).generateMessageId();
        Message message = new Message().withBody(messageBody).withMessageId(String.valueOf(messageId));
        addMessageToEndOfFile(Collections.singletonList(message), getMessagesFile(queueUrl));
        return messageId;
    }

    //done
    @Override
    public Optional<Message> receiveMessage(String queueUrl) {
        invalidateInflight(queueUrl);
        return removeMessagesFromFile(getMessagesFile(queueUrl), FIRST_MESSAGE_EXTRACTOR)
                .stream()
                .findFirst()
                .map(message -> {
                    String receiptHandle = getIdsGenerator(queueUrl).generateRecipientHandlerId(message);
                    message.withReceiptHandle(receiptHandle);
                    addMessageToEndOfFile(Collections.singletonList(message), getInflightFile(queueUrl));
                    return message;
                });
    }

    private void invalidateInflight(String queueUrl) {
        long curTime = System.currentTimeMillis();
        addMessagesToBeginningOfFile(
                removeMessagesFromFile(getInflightFile(queueUrl),
                        BY_INFLIGHT_DELAY_SPLITTER.apply(getInflightDelay(), curTime)),
                getMessagesFile(queueUrl)
        );
    }

    @Override
    public void deleteMessage(String queueUrl, String receiptHandle) {
        removeMessagesFromFile(getInflightFile(queueUrl), BY_RECEIPT_HANDLER_SPLITTER.apply(receiptHandle));
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

    public static void main(String[] args) throws IOException {
        Path rootPath = Paths.get("/tmp/dirr");
        Files.walk(rootPath, FileVisitOption.FOLLOW_LINKS)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .peek(System.out::println)
                .forEach(File::delete);
    }

    @SuppressWarnings("WeakerAccess")
    public static void init(String queueName) {
        String queuesBaseDirStr = SQSRunner.getInstance().getProperty(SQS_QUEUES_DIR_KEY);

        Path queueDir = Paths.get(queuesBaseDirStr, queueName);
        Path messagesFile = getMessagesFile(queueDir.toString());
        Path inflightFile = getInflightFile(queueDir.toString());
        Path configDir = getConfigDir(queueDir.toString());

        try (GlobalCloseableLock ignored = new GlobalCloseableLock(queuesBaseDirStr).lock()) {
            if (!Files.exists(queueDir)) {
                Files.createDirectory(queueDir);
                Files.createDirectory(configDir);
                Files.createFile(messagesFile);
                Files.createFile(inflightFile);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
