package com.sfdcupload.file.source.fs;

import com.sfdcupload.file.domain.MigrationFile;
import com.sfdcupload.file.source.FileSource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

@Component
public class FileSystemFileSource implements FileSource {

    @Override
    public List<MigrationFile> loadFiles(Path rootPath) throws IOException {
        if (Files.notExists(rootPath)) {
            return List.of();
        }

        try (Stream<Path> pathStream = Files.walk(rootPath)) {
            return pathStream
                    .filter(Files::isRegularFile)
                    .sorted(Comparator.comparing(Path::toString))
                    .map(this::toMigrationFile)
                    .toList();
        }
    }

    private MigrationFile toMigrationFile(Path path) {
        try {
            byte[] content = Files.readAllBytes(path);
            return MigrationFile.builder()
                    .sourceId(path.toString())
                    .fileName(path.getFileName().toString())
                    .content(content)
                    .size(content.length)
                    .build();
        } catch (IOException e) {
            throw new IllegalStateException("파일을 읽는 중 오류가 발생했습니다: " + path, e);
        }
    }
}
