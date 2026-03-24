package com.sfdcupload.file.source;

import com.sfdcupload.file.domain.MigrationFile;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public interface FileSource {
    List<MigrationFile> loadFiles(Path rootPath) throws IOException;
}
