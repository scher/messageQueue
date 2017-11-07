package com.canva.sqs.local.filesystem;

import com.amazonaws.services.sqs.model.Message;
import com.canva.sqs.local.IdsGenerator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

/**
 * @author Alexander Pronin
 * @since 06/11/2017
 */
public class FileIdsGenerator implements IdsGenerator {

    private final Path file;

    @SuppressWarnings("WeakerAccess")
    public FileIdsGenerator(Path file) {
        this.file = file;
    }

    @Override
    public String generateMessageId() {
        return getByIndexAndIncrement(0);
    }

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
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
