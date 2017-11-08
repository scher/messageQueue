package com.canva.sqs.local.filesystem;

import com.amazonaws.services.sqs.model.Message;
import com.canva.sqs.local.IdsGenerator;
import org.apache.http.annotation.ThreadSafe;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * {@inheritDoc}
 * <p>
 * Durable Ids Generator that stores current state in file.
 * <p>
 * Ids generation is suitable for single-host usage.
 * All methods are atomic (in scope of single-host) and thread-safe.
 * <p>
 * This implementation leverages {@link GlobalCloseableLock} to achieve exclusive locking during id generation.
 *
 * @author Alexander Pronin
 * @see GlobalCloseableLock
 * @since 06/11/2017
 */
@ThreadSafe
public class FileIdsGenerator implements IdsGenerator {

    private final Path file;

    /**
     * @param file file that will be used to store state of id generator
     */
    @SuppressWarnings("WeakerAccess")
    public FileIdsGenerator(Path file) {
        this.file = file;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Id generation is atomic.
     */
    @Override
    public String generateMessageId() {
        return getByIndexAndIncrement(0);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Atomically generates RecipientHandler.
     * For the purpose of simplicity message content is not used for generation.
     */
    @Override
    public String generateRecipientHandlerId(Message message) {
        return getByIndexAndIncrement(1);
    }

    private String getByIndexAndIncrement(int index) {
        String result = "";
        try (GlobalCloseableLock ignored = new GlobalCloseableLock(file.toString()).lock()) {
            List<String> ids = Files.readAllLines(file);
            if (ids.isEmpty()) {
                ids = Arrays.asList("0", "0");
            }
            result = ids.get(index);
            ids.set(index, String.valueOf(Integer.valueOf(result) + 1));
            Files.write(file, ids, WRITE, TRUNCATE_EXISTING);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
