package com.example.claims;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Compact open-addressing hash map from a numeric id (long) to a byte[]
 * payload, built for ~250M entries on a single machine.
 *
 * Memory optimizations vs PackedJoinTable:
 *  - Keys stored as primitive long (8 bytes), no per-key byte arena.
 *    Member/provider ids are "MBR"/"PRV" + digits, so they parse to a long.
 *  - Payload offset+length packed into ONE long (offset in high bits).
 *  - Payloads in a segmented byte arena (Java arrays cap at ~2.1 GB).
 *  - Load factor ~0.75 keeps the slots array small.
 *
 * Footprint is roughly (8 key + 8 packed + payloadLen) bytes/entry plus
 * the slots array, versus ~2x that for the general PackedJoinTable.
 */
public final class LongKeyJoinTable {

    private static final long EMPTY = Long.MIN_VALUE;
    private static final int SEGMENT_SIZE = 1 << 28; // 256 MB
    private static final int SEGMENT_MASK = SEGMENT_SIZE - 1;

    private long[] keys;        // slot -> key, or EMPTY
    private long[] payloadRef;  // slot -> packed (segmentIndex << 40 | offset << 12 | length)
    private final int mask;
    private int size;

    private final List<byte[]> payloadSegments = new ArrayList<>();
    private long payloadArenaLen;

    public LongKeyJoinTable(int expectedEntries) {
        int cap = 1;
        while (cap < expectedEntries * 1.15 && cap > 0) cap <<= 1; // load factor <= ~0.87
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
        // skip non-digit prefix
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

    private long appendPayload(byte[] payload) {
        long off = payloadArenaLen;
        int segIdx = (int) (off / SEGMENT_SIZE);
        int segOff = (int) (off & SEGMENT_MASK);
        if (segOff + payload.length > SEGMENT_SIZE) {
            off = (long) (segIdx + 1) * SEGMENT_SIZE;
            payloadArenaLen = off;
            segIdx++;
            segOff = 0;
        }
        while (payloadSegments.size() <= segIdx) payloadSegments.add(new byte[SEGMENT_SIZE]);
        System.arraycopy(payload, 0, payloadSegments.get(segIdx), segOff, payload.length);
        payloadArenaLen = off + payload.length;
        // pack: segment index (high 24 bits) | offset (mid 28 bits) | length (low 28 bits... use separate)
        // simpler: store global offset, length separately is not available -> encode length in high bits
        // global offset fits in ~35 bits for our sizes; length fits in ~8 bits (<=255)
        return ((long) payload.length << 40) | off;
    }

    private byte[] readPayload(long ref) {
        int len = (int) (ref >>> 40);
        long off = ref & 0xFFFFFFFFFFL; // low 40 bits
        int segIdx = (int) (off / SEGMENT_SIZE);
        int segOff = (int) (off & SEGMENT_MASK);
        byte[] out = new byte[len];
        System.arraycopy(payloadSegments.get(segIdx), segOff, out, 0, len);
        return out;
    }

    private static int hash(long key) {
        long h = key * 0x9E3779B97F4A7C15L;
        h ^= h >>> 33;
        return (int) (h & 0x7FFFFFFF);
    }
}
