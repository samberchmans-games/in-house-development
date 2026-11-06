package com.example.claims;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Memory-efficient open-addressing hash table mapping a String key to a
 * packed byte[] payload, designed for hundreds of millions of entries.
 *
 * Two design points that matter at this scale:
 *  - Flat byte arenas instead of HashMap<String,String[]> (~50 bytes/entry
 *    vs ~200), avoiding GC pressure and improving cache locality.
 *  - SEGMENTED arenas: a single Java array is limited to ~2.1 billion
 *    elements, so 250M entries x ~40 bytes (~10 GB) cannot fit in one
 *    byte[]. Keys and payloads are stored across a list of byte[] segments
 *    (each <= SEGMENT_SIZE), addressed by a global long offset.
 */
public final class PackedJoinTable {

    private static final int EMPTY = -1;
    private static final int SEGMENT_SIZE = 1 << 28; // 256 MB per segment
    private static final int SEGMENT_MASK = SEGMENT_SIZE - 1;

    private int[] slots;          // hash -> entry index, or EMPTY
    private long[] keyOff;        // entry -> global offset in key segments
    private int[] keyLen;         // entry -> key length
    private long[] payloadOff;    // entry -> global offset in payload segments
    private int[] payloadLen;     // entry -> payload length
    private int size;
    private final int entryCap;

    private final List<byte[]> keySegments = new ArrayList<>();
    private long keyArenaLen;
    private final List<byte[]> payloadSegments = new ArrayList<>();
    private long payloadArenaLen;

    public PackedJoinTable(int expectedEntries) {
        int cap = 1;
        while (cap < expectedEntries * 2L && cap > 0) cap <<= 1;   // load factor <= 0.5
        slots = new int[cap];
        Arrays.fill(slots, EMPTY);
        entryCap = expectedEntries + 16;
        keyOff = new long[entryCap];
        keyLen = new int[entryCap];
        payloadOff = new long[entryCap];
        payloadLen = new int[entryCap];
    }

    public int size() { return size; }

    public void put(String key, byte[] payload) {
        if (size == entryCap) throw new IllegalStateException("entry capacity exceeded");
        byte[] kb = key.getBytes(StandardCharsets.UTF_8);

        long kOff = appendKey(kb);
        long pOff = appendPayload(payload);

        int e = size++;
        keyOff[e] = kOff;    keyLen[e] = kb.length;
        payloadOff[e] = pOff; payloadLen[e] = payload.length;

        int mask = slots.length - 1;
        int h = hash(kb) & mask;
        while (slots[h] != EMPTY) h = (h + 1) & mask;
        slots[h] = e;
    }

    /** Returns payload for key, or null. */
    public byte[] get(String key) {
        byte[] kb = key.getBytes(StandardCharsets.UTF_8);
        int mask = slots.length - 1;
        int h = hash(kb) & mask;
        while (true) {
            int e = slots[h];
            if (e == EMPTY) return null;
            if (keyLen[e] == kb.length && matches(e, kb)) {
                return readPayload(e);
            }
            h = (h + 1) & mask;
        }
    }

    // ---- segmented key arena ----

    private long appendKey(byte[] kb) {
        long off = keyArenaLen;
        int segIdx = (int) (off / SEGMENT_SIZE);
        int segOff = (int) (off & SEGMENT_MASK);
        // if the key would straddle a segment boundary, move to a fresh segment
        if (segOff + kb.length > SEGMENT_SIZE) {
            off = (long) (segIdx + 1) * SEGMENT_SIZE;
            keyArenaLen = off;
            segIdx++;
            segOff = 0;
        }
        while (keySegments.size() <= segIdx) keySegments.add(new byte[SEGMENT_SIZE]);
        System.arraycopy(kb, 0, keySegments.get(segIdx), segOff, kb.length);
        keyArenaLen = off + kb.length;
        return off;
    }

    private boolean matches(int e, byte[] kb) {
        long off = keyOff[e];
        int segIdx = (int) (off / SEGMENT_SIZE);
        int segOff = (int) (off & SEGMENT_MASK);
        byte[] seg = keySegments.get(segIdx);
        for (int i = 0; i < kb.length; i++) {
            if (seg[segOff + i] != kb[i]) return false;
        }
        return true;
    }

    // ---- segmented payload arena ----

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
        return off;
    }

    private byte[] readPayload(int e) {
        long off = payloadOff[e];
        int segIdx = (int) (off / SEGMENT_SIZE);
        int segOff = (int) (off & SEGMENT_MASK);
        byte[] out = new byte[payloadLen[e]];
        System.arraycopy(payloadSegments.get(segIdx), segOff, out, 0, out.length);
        return out;
    }

    private static int hash(byte[] b) {
        int h = 0x811C9DC5;
        for (byte x : b) { h ^= x & 0xFF; h *= 0x01000193; }
        h ^= h >>> 16;
        return h & 0x7FFFFFFF;
    }
}
