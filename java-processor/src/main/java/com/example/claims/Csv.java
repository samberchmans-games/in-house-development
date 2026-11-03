package com.example.claims;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.zip.GZIPInputStream;

/** Minimal streaming reader for partitioned .csv.gz datasets (no quoting in our data). */
public final class Csv {

    private Csv() {}

    public static List<Path> parts(Path dir) throws IOException {
        List<Path> out = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "*.csv.gz")) {
            for (Path p : ds) out.add(p);
        }
        out.sort(null);
        return out;
    }

    /** Iterates rows of every part file in order, skipping headers. Each row is a raw String[]. */
    public static Iterable<String[]> rows(Path dir) {
        return () -> new Iterator<String[]>() {
            private final List<Path> files = init();
            private int fileIdx = 0;
            private BufferedReader reader;
            private String[] next;
            private boolean finished;

            private List<Path> init() {
                try {
                    return parts(dir);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            private void advance() {
                if (finished) return;
                try {
                    while (true) {
                        if (reader == null) {
                            if (fileIdx >= files.size()) { finished = true; return; }
                            reader = new BufferedReader(new InputStreamReader(
                                    new GZIPInputStream(Files.newInputStream(files.get(fileIdx++)),
                                            1 << 16),
                                    StandardCharsets.UTF_8), 1 << 16);
                            reader.readLine(); // header
                        }
                        String line = reader.readLine();
                        if (line == null) { reader.close(); reader = null; continue; }
                        next = line.split(",", -1);
                        return;
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override public boolean hasNext() {
                if (next == null) advance();
                return !finished;
            }

            @Override public String[] next() {
                if (!hasNext()) throw new NoSuchElementException();
                String[] r = next;
                next = null;
                return r;
            }
        };
    }
}
