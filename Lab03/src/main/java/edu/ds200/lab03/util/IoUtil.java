package edu.ds200.lab03.util;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class IoUtil {
  private IoUtil() {}

  public static void writeLines(Path file, List<String> lines) throws IOException {
    Path parent = file.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    Files.write(file, lines, StandardCharsets.UTF_8);
  }
}
