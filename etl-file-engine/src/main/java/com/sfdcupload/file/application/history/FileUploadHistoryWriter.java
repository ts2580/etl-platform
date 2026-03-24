package com.sfdcupload.file.application.history;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.sfdcupload.file.domain.result.FileUploadSummary;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Component
public class FileUploadHistoryWriter {
    private static final DateTimeFormatter FILE_TS = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private static final Path HISTORY_DIR = Paths.get("/mnt/efs/sfdc-upload-history");

    public Path write(FileUploadSummary summary) throws IOException {
        Files.createDirectories(HISTORY_DIR);
        Path target = HISTORY_DIR.resolve("upload-" + LocalDateTime.now().format(FILE_TS) + ".json");
        MAPPER.writeValue(target.toFile(), summary);
        return target;
    }
}
