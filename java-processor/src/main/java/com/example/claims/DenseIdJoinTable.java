package com.example.claims;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Direct-indexed lookup table from a numeric id to an encoded payload (see Codecs),
 * for reference tables whose ids are (mostly) dense: MBR000000001..MBR250000000.
 *
 * Why not a hash table (OnHeapJoinTable)?
 *  - OnHeapJoinTable sized for 250M entries rounds up to 2^29 slots and keeps
 *    TWO long[] arrays (keys + refs) = 8 GiB of index PER TABLE, before any
 *    payload. Two tables = 16 GiB of index + ~19 GiB payload = ~35 GiB, which
 *    does not fit a 30 GiB heap (or a 32 GiB machine).
 *  - When ids are dense, the id IS the slot. No keys array, no hashing, no
 *    probing: one long[maxId+1] = 1.9 GiB per table (measured).
 *  - Measured, full data: index 16 GiB -> 3.8 GiB for both tables. With text
 *    payloads the tables were 23.0 GiB (still too big for a 32 GB Mac); with
 *    {@link Codecs} they are 11.4 GiB, which is what made the 12.6 min run possible.
 *  - Lookups also got cheaper: one array read instead of hash + linear probe
 *    over two 4 GiB arrays (two cache misses per probe).
 *  - Ids above maxId (unexpected / sparse outliers) go to a small overflow
 *    map instead of failing, and are reported.
 *
 * Loading is parallel: each loader thread gets its own {@link Loader} which
 * appends into private arena segments, so threads never contend on the hot
 * path. Only allocating a new 16 MB segment is synchronized.
 *
 * Duplicate ids (the generator emits each part-boundary id twice, with
 * different attributes) are resolved deterministically: the row from the
 * LOWEST part index wins, matching a sequential "first row wins" load
 * regardless of thread timing. Duplicates are counted and reported.
 *
 * Thread-safety contract: load with any number of Loaders concurrently, then
 * establish happens-before (e.g. Future.get()) and call {@link #freeze()}
 * before any lookups.
 */
public final class DenseIdJoinTable {

    /**
     * ~16 MB segments. Deliberately a bit UNDER 16 MiB: with G1 an array of
     * exactly 16 MiB + 16-byte header needs a third 8 MiB region (50% waste).
     * Small segments also keep the per-loader partially-filled tail small.
     */
    static final int SEGMENT_SIZE = (1 << 24) - 4096;
    private static final int MAX_PAYLOAD = 255; // length stored in 8 bits

    private static final int MAX_PARTS = 1 << 12;
    private static final VarHandle REFS = MethodHandles.arrayElementVarHandle(long[].class);

    // ref layout (64 bits):  part(12) | segmentIndex+1 (20) | segmentOffset(24) | length(8) ; 0 = absent
    private final long[] refs;
    private final ConcurrentHashMap<Long, Long> overflow = new ConcurrentHashMap<>();
    private final List<byte[]> segmentList = new ArrayList<>(); // guarded by this
    private byte[][] segments; // set by freeze()

    private final AtomicLong size = new AtomicLong();
    private final AtomicLong payloadBytes = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();

    public DenseIdJoinTable(int maxId) {
        refs = new long[maxId + 1];
    }

    /** One per part file / loading thread; part decides who wins on duplicate ids. Not thread-safe itself. */
    public Loader newLoader(int part) {
        if (part < 0 || part >= MAX_PARTS) throw new IllegalArgumentException("part index " + part);
        return new Loader(part);
    }

    private static int partOf(long ref) { return (int) (ref >>> 52); }

    public final class Loader {
        private final long partBits;
        private final int part;
        private Loader(int part) { this.part = part; this.partBits = (long) part << 52; }

        private byte[] seg;
        private int segIdx = -1;
        private int pos;
        private long added, bytes, dups;

        public void put(long id, byte[] payload) {
            int len = payload.length;
            if (len > MAX_PAYLOAD) {
                throw new IllegalArgumentException("payload " + len + " bytes > " + MAX_PAYLOAD + " for id " + id);
            }
            if (seg == null || pos + len > SEGMENT_SIZE) newSegment();
            if (segIdx + 1 >= (1 << 20)) throw new IllegalStateException("too many segments");
            System.arraycopy(payload, 0, seg, pos, len);
            long ref = partBits | ((long) (segIdx + 1) << 32) | ((long) pos << 8) | len;
            pos += len;
            bytes += len;
            if (id >= 0 && id < refs.length) {
                int i = (int) id;
                while (true) {
                    long cur = (long) REFS.getVolatile(refs, i);
                    if (cur == 0) {
                        if (REFS.compareAndSet(refs, i, 0L, ref)) { added++; return; }
                    } else {
                        dups++;
                        if (partOf(cur) <= part) return;              // existing row wins
                        if (REFS.compareAndSet(refs, i, cur, ref)) return; // we win
                        dups--;                                       // raced; retry
                    }
                }
            }
            Long boxed = id;
            Long prev = overflow.putIfAbsent(boxed, ref);
            if (prev == null) { added++; return; }
            dups++;
            while (partOf(prev) > part && !overflow.replace(boxed, prev, ref)) prev = overflow.get(boxed);
        }

        private void newSegment() {
            synchronized (DenseIdJoinTable.this) {
                seg = new byte[SEGMENT_SIZE];
                segIdx = segmentList.size();
                segmentList.add(seg);
            }
            pos = 0;
        }

        /** Publish this loader's counters. Call once when the loader is done. */
        public void finish() {
            size.addAndGet(added);
            payloadBytes.addAndGet(bytes);
            duplicates.addAndGet(dups);
            added = 0;
            bytes = 0;
            dups = 0;
        }
    }

    /** Call after all loaders finished (and are visible to this thread). */
    public synchronized void freeze() {
        segments = segmentList.toArray(new byte[0][]);
    }

    public long size() { return size.get(); }
    public int overflowSize() { return overflow.size(); }
    /** Rows whose id was already present (only one row per id is kept). */
    public long duplicates() { return duplicates.get(); }
    public long payloadBytes() { return payloadBytes.get(); }
    public long segmentBytes() { return (long) segments.length * SEGMENT_SIZE; }
    public long indexBytes() { return (long) refs.length * 8; }

    /** Looks up id and decodes its payload into out via codec. Returns false if absent. */
    public boolean get(long id, Codecs.Codec codec, String[] out) {
        long ref;
        if (id >= 0 && id < refs.length) {
            ref = refs[(int) id];
        } else {
            Long r = overflow.isEmpty() ? null : overflow.get(id);
            ref = r == null ? 0 : r;
        }
        if (ref == 0) return false;
        byte[] seg = segments[(int) ((ref >>> 32) & 0xFFFFF) - 1];
        codec.decode(seg, (int) ((ref >>> 8) & 0xFFFFFF), (int) (ref & 0xFF), out);
        return true;
    }
}
