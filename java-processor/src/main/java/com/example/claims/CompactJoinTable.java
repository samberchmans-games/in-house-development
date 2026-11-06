package com.example.claims;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Compact open-addressing hash map from numeric id (long) to a string payload,
 * designed so TWO 250M-entry tables fit in ~30 GB (enabling single-pass join).
 *
 * Memory optimizations vs LongKeyJoinTable:
 *  - Keys: primitive long[] (8 bytes/entry).
 *  - Payloads: stored OFF-HEAP in direct ByteBuffer segments (no GC pressure,
 *    no heap footprint). Addressed by a packed long (segment<<40|offset<<8|len).
 *  - Single payloadRef long[] (8 bytes/entry) - no separate offset/length arrays.
 *  - Load factor ~0.8.
 *
 * Footprint per table ~ (8 key + 8 ref) * capacity + payload bytes (off-heap).
 */
public final class CompactJoinTable {

    private static final long EMPTY = Long.MIN_VALUE;
    private static final int SEGMENT_SIZE = 1 << 28; // 256 MB direct buffers
    private static final int SEGMENT_MASK = SEGMENT_SIZE - 1;

    private long[] keys;
    private long[] payloadRef;  // packed: length(8 bits) << 56 | segment(8) << 48 ... simpler: globalOffset(40) | len
    private final int mask;
    private int size;

    private final List<ByteBuffer> segments = new ArrayList<>();
    private long arenaLen;

    public CompactJoinTable(int expectedEntries) {
        int cap = 1;
        while (cap < expectedEntries * 1.25 && cap > 0) cap <<= 1; // load factor ~0.8
        keys = new long[cap];
        Arrays.fill(keys, EMPTY);
        payloadRef = new long[cap];
        mask = cap - 1;
    }

    public int size() { return size; }

    /** Parse "MBR000000123" / "PRV000000123" -> 123. Returns -1 if not parseable. */
    public static long parseId(String id) {
        if (id == null) return -1;
        int n = id.length();
        long v = 0;
        int i = 0;
        while (i < n && !Character.isDigit(id.charAt(i))) i++;
        if (i == n) return -1;
        for (; i < n; i++) {
            char c = id.charAt(i);
            if (c < '0' || c > '9') return -1;
            v = v * 10 + (c - '0');
        }
        return v;
    }

    public void put(long key, byte[] payload) {
        long ref = appendPayload(payload);
        int h = hash(key) & mask;
        while (keys[h] != EMPTY) h = (h + 1) & mask;
        keys[h] = key;
        payloadRef[h] = ref;
        size++;
    }

    /** Returns payload for key, or null. */
    public byte[] get(long key) {
        int h = hash(key) & mask;
        while (true) {
            long k = keys[h];
            if (k == EMPTY) return null;
            if (k == key) return readPayload(payloadRef[h]);
            h = (h + 1) & mask;
        }
    }

    /** Writes the payload for key into a StringBuilder, with '|' kept as-is (raw). */
    public void appendPayloadTo(long key, StringBuilder sb) {
        appendPayloadTo(key, sb, false);
    }

    /**
     * Writes the payload for key into a StringBuilder.
     * @param csvMode if true, '|' is written as ',' (for CSV output); if false, '|' is preserved.
     */
    public void appendPayloadTo(long key, StringBuilder sb, boolean csvMode) {
        int h = hash(key) & mask;
        while (true) {
            long k = keys[h];
            if (k == EMPTY) return;
            if (k == key) {
                long ref = payloadRef[h];
                int len = (int) (ref >>> 40);
                long off = ref & 0xFFFFFFFFFFL;
                int segIdx = (int) (off / SEGMENT_SIZE);
                int segOff = (int) (off & SEGMENT_MASK);
                ByteBuffer seg = segments.get(segIdx).duplicate();
                seg.position(segOff);
                for (int i = 0; i < len; i++) {
                    byte b = seg.get();
                    if (csvMode && b == '|') sb.append(',');
                    else sb.append((char) (b & 0xFF));
                }
                return;
            }
            h = (h + 1) & mask;
        }
    }

    private long appendPayload(byte[] payload) {
        long off = arenaLen;
        int segIdx = (int) (off / SEGMENT_SIZE);
        int segOff = (int) (off & SEGMENT_MASK);
        if (segOff + payload.length > SEGMENT_SIZE) {
            off = (long) (segIdx + 1) * SEGMENT_SIZE;
            arenaLen = off;
            segIdx++;
            segOff = 0;
        }
        while (segments.size() <= segIdx) {
            segments.add(ByteBuffer.allocateDirect(SEGMENT_SIZE));
        }
        ByteBuffer seg = segments.get(segIdx).duplicate();
        seg.position(segOff);
        seg.put(payload);
        arenaLen = off + payload.length;
        return ((long) payload.length << 40) | off;
    }

    private byte[] readPayload(long ref) {
        int len = (int) (ref >>> 40);
        long off = ref & 0xFFFFFFFFFFL;
        int segIdx = (int) (off / SEGMENT_SIZE);
        int segOff = (int) (off & SEGMENT_MASK);
        ByteBuffer seg = segments.get(segIdx).duplicate();
        seg.position(segOff);
        byte[] out = new byte[len];
        seg.get(out);
        return out;
    }

    private static int hash(long key) {
        long h = key * 0x9E3779B97F4A7C15L;
        h ^= h >>> 33;
        return (int) (h & 0x7FFFFFFF);
    }
}
