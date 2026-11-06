package com.example.claims;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * On-heap open-addressing hash map from numeric id (long) to a byte[] payload,
 * built so TWO 250M-entry tables fit in the heap for a single-pass join.
 *
 * Design:
 *  - Keys: primitive long[] (8 bytes/entry), no per-key object.
 *  - Payloads: on-heap byte[] SEGMENTS (each <= 256 MB, since one array caps at
 *    ~2 GB), addressed by a packed long (globalOffset << 8 | length).
 *  - On-heap byte[] builds far faster than off-heap direct buffers (arraycopy
 *    vs per-byte put) - and uses the same physical RAM. Off-heap does NOT reduce
 *    total memory; to fit more data you must shrink it, not move it off-heap.
 */
public final class OnHeapJoinTable {

    private static final long EMPTY = Long.MIN_VALUE;
    private static final int SEGMENT_SIZE = 1 << 28; // 256 MB
    private static final int SEGMENT_MASK = SEGMENT_SIZE - 1;

    private final long[] keys;
    private final long[] payloadRef;   // packed: (globalOffset << 8) | length
    private final List<byte[]> segments = new ArrayList<>();
    private long arenaLen;
    private final int mask;
    private int size;

    public OnHeapJoinTable(int expectedEntries) {
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
        long off = arenaLen;
        int segIdx = (int) (off / SEGMENT_SIZE);
        int segOff = (int) (off & SEGMENT_MASK);
        if (segOff + payload.length > SEGMENT_SIZE) {
            off = (long) (segIdx + 1) * SEGMENT_SIZE;
            arenaLen = off;
            segIdx++;
            segOff = 0;
        }
        while (segments.size() <= segIdx) segments.add(new byte[SEGMENT_SIZE]);
        System.arraycopy(payload, 0, segments.get(segIdx), segOff, payload.length);
        arenaLen = off + payload.length;

        int h = hash(key) & mask;
        while (keys[h] != EMPTY) h = (h + 1) & mask;
        keys[h] = key;
        payloadRef[h] = (off << 8) | (payload.length & 0xFF);
        size++;
    }

    /** Appends the payload for key to sb ('|' preserved). Returns true if found. */
    public boolean appendPayloadTo(long key, StringBuilder sb) {
        int h = hash(key) & mask;
        while (true) {
            long k = keys[h];
            if (k == EMPTY) return false;
            if (k == key) {
                long ref = payloadRef[h];
                int len = (int) (ref & 0xFF);
                long off = ref >>> 8;
                int segIdx = (int) (off / SEGMENT_SIZE);
                int segOff = (int) (off & SEGMENT_MASK);
                byte[] seg = segments.get(segIdx);
                for (int i = 0; i < len; i++) {
                    sb.append((char) (seg[segOff + i] & 0xFF));
                }
                return true;
            }
            h = (h + 1) & mask;
        }
    }

    private static int hash(long key) {
        long h = key * 0x9E3779B97F4A7C15L;
        h ^= h >>> 33;
        return (int) (h & 0x7FFFFFFF);
    }
}
