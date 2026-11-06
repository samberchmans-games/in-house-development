package com.example.claims;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "*.csv*")) {
            for (Path p : ds) out.add(p);
        }
        out.sort(null);
        return out;
    }

    /** Iterates rows of a single .csv.gz file, skipping its header. */
    public static Iterable<String[]> rowsOfFile(Path file) {
        return () -> new Iterator<String[]>() {
            private BufferedReader reader;
            private String[] next;
            private boolean finished;

            private void open() {
                try {
                    reader = new BufferedReader(new InputStreamReader(
                            new GZIPInputStream(Files.newInputStream(file), 1 << 16),
                            StandardCharsets.UTF_8), 1 << 16);
                    reader.readLine(); // header
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            private void advance() {
                if (finished) return;
                if (reader == null) open();
                try {
                    String line = reader.readLine();
                    if (line == null) { finished = true; reader.close(); return; }
                    next = line.split(",", -1);
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

    /** Iterates rows of a single plain (uncompressed) .csv file, skipping its header. */
    public static Iterable<String[]> rowsOfPlainFile(Path file) {
        return () -> new Iterator<String[]>() {
            private BufferedReader reader;
            private String[] next;
            private boolean finished;

            private void open() {
                try {
                    reader = Files.newBufferedReader(file, StandardCharsets.UTF_8);
                    reader.readLine(); // header
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            private void advance() {
                if (finished) return;
                if (reader == null) open();
                try {
                    String line = reader.readLine();
                    if (line == null) { finished = true; reader.close(); return; }
                    next = line.split(",", -1);
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

    /**
     * Memory-mapped reader for a single plain (uncompressed) .csv file.
     * Maps the file into memory via FileChannel.map and scans bytes directly,
     * avoiding read() syscalls and BufferedReader overhead. Only valid for
     * uncompressed files (gzip streams cannot be memory-mapped).
     */
    public static Iterable<String[]> rowsMapped(Path file) {
        return () -> new Iterator<String[]>() {
            private java.nio.MappedByteBuffer buf = map();
            private String[] next;
            private boolean finished;
            private boolean headerSkipped;

            private java.nio.MappedByteBuffer map() {
                try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
                    return ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            private String readLine() {
                if (!buf.hasRemaining()) return null;
                int start = buf.position();
                int limit = buf.limit();
                int i = start;
                while (i < limit) {
                    byte b = buf.get(i);
                    if (b == '\n') break;
                    i++;
                }
                int len = i - start;
                // strip trailing \r if present
                if (len > 0 && buf.get(start + len - 1) == '\r') len--;
                byte[] bytes = new byte[len];
                java.nio.ByteBuffer dup = buf.duplicate();
                dup.position(start);
                dup.get(bytes, 0, len);
                buf.position(i < limit ? i + 1 : limit);
                return new String(bytes, StandardCharsets.UTF_8);
            }

            private void advance() {
                if (finished) return;
                if (!headerSkipped) { readLine(); headerSkipped = true; }
                String line = readLine();
                if (line == null) { finished = true; return; }
                next = line.split(",", -1);
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
