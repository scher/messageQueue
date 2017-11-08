package com.canva.sqs.local.filesystem;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Resolves various internal queue files by queueUrl
 *
 * @author Alexander Pronin
 * @since 08/11/2017
 */
public enum FileDescriptor {
    MESSAGES("messages"),
    INFLIGHT("inflight"),
    IDS_CONFIG("ids"),
    SEMAPHORE("semaphore") {
        @Override
        public Path getPath(String queueUrl) {
            return Paths.get(queueUrl + ".semaphore");
        }
    };

    private final String name;

    FileDescriptor(String name) {
        this.name = name;
    }

    public Path getPath(String queueUrl) {
        return Paths.get(queueUrl, name);
    }
}
