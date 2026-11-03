package com.example.claims;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Memory-efficient open-addressing hash table mapping a String key to a
 * packed byte[] payload, both stored in flat structures to avoid the
 * ~200 bytes/entry overhead of HashMap<String,String[]>.
 *
 * Keys are stored as byte[] in a single growable byte arena; the table
 * holds (arenaOffset, keyLen, payloadIndex) per slot. Payloads live in a
 * separate growable byte arena. Roughly 40-60 bytes/entry all-in.
 */
public final class PackedJoinTable {

    private static final int EMPTY = -1;

    private int[] slots;          // hash -> index into entries, or EMPTY
    private int[] keyArenaOff;    // entry -> offset in keyArena
    private int[] keyLen;         // entry -> key length
    private int[] payloadOff;     // entry -> offset in payloadArena
    private int[] payloadLen;     // entry -> payload length
    private int size;
    private int entryCap;

    private byte[] keyArena;
    private int keyArenaLen;
    private byte[] payloadArena;
    private int payloadArenaLen;

    public PackedJoinTable(int expectedEntries) {
        int cap = 1;
        while (cap < expectedEntries * 2) cap <<= 1;   // load factor <= 0.5
        slots = new int[cap];
        Arrays.fill(slots, EMPTY);
        entryCap = expectedEntries + 16;
        keyArenaOff = new int[entryCap];
        keyLen = new int[entryCap];
        payloadOff = new int[entryCap];
        payloadLen = new int[entryCap];
        keyArena = new byte[expectedEntries * 12];     // ~12 bytes/key avg
        payloadArena = new byte[expectedEntries * 40]; // ~40 bytes/payload avg
    }

    public int size() { return size; }

    public void put(String key, byte[] payload) {
        if (size == entryCap) throw new IllegalStateException("entry capacity exceeded");
        byte[] kb = key.getBytes(StandardCharsets.UTF_8);
        ensureKeyArena(kb.length);
        ensurePayloadArena(payload.length);

        int kOff = keyArenaLen;
        System.arraycopy(kb, 0, keyArena, kOff, kb.length);
        keyArenaLen += kb.length;
        int pOff = payloadArenaLen;
        System.arraycopy(payload, 0, payloadArena, pOff, payload.length);
        payloadArenaLen += payload.length;

        int e = size++;
        keyArenaOff[e] = kOff; keyLen[e] = kb.length;
        payloadOff[e] = pOff;  payloadLen[e] = payload.length;

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
                byte[] out = new byte[payloadLen[e]];
                System.arraycopy(payloadArena, payloadOff[e], out, 0, out.length);
                return out;
            }
            h = (h + 1) & mask;
        }
    }

    private boolean matches(int e, byte[] kb) {
        int off = keyArenaOff[e];
        for (int i = 0; i < kb.length; i++) {
            if (keyArena[off + i] != kb[i]) return false;
        }
        return true;
    }

    private static int hash(byte[] b) {
        int h = 0x811C9DC5;
        for (byte x : b) { h ^= x & 0xFF; h *= 0x01000193; }
        h ^= h >>> 16;
        return h & 0x7FFFFFFF;
    }

    private void ensureKeyArena(int more) {
        if (keyArenaLen + more > keyArena.length) {
            keyArena = Arrays.copyOf(keyArena, Math.max(keyArena.length * 2, keyArenaLen + more));
        }
    }

    private void ensurePayloadArena(int more) {
        if (payloadArenaLen + more > payloadArena.length) {
            payloadArena = Arrays.copyOf(payloadArena, Math.max(payloadArena.length * 2, payloadArenaLen + more));
        }
    }
}
