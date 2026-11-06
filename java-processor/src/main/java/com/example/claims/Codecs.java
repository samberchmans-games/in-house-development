package com.example.claims;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Compact payload encodings for the reference tables.
 *
 * Why: with the plain "a|b|c" text payload the two tables need ~23 GiB, which a
 * 32 GB Mac will not keep resident - the join then crawls on swap. The data is
 * highly redundant, so encode it:
 *
 *  provider  (~59 B -> ~15 B):
 *    [0][specialty dict][network dict][npi: 5-byte int][nameMode][name bytes]
 *    - specialty (20 values) / network_status (3 values): 1-byte dictionary codes
 *    - npi: always 10 digits -> 40-bit integer instead of 10 ASCII bytes
 *    - provider_name is "<LAST> <SPECIALTY> CLINIC" for ~96% of rows, so store
 *      only "<LAST>" and rebuild the rest from the specialty on lookup
 *
 *  member    (~23 B -> ~17 B):
 *    [0][gender dict][plan dict][risk_score as hundredths, 2 bytes][first|last]
 *
 * Any row that does not fit the compact shape (unexpected npi, too many
 * distinct values, odd risk_score...) is stored VERBATIM as [1]a|b|c, so the
 * codec is lossless for every input, not just for the happy path.
 */
public final class Codecs {

    private Codecs() {}

    private static final byte COMPACT = 0, VERBATIM = 1;

    /** Turns raw CSV row columns into payload bytes, and back into output fields. */
    public interface Codec {
        byte[] encode(String[] row);
        void decode(byte[] b, int off, int len, String[] out);
        int fields();
    }

    /** Thread-safe string dictionary, code 0 = empty string, codes 1..255. */
    static final class Dict {
        private final ConcurrentHashMap<String, Integer> codes = new ConcurrentHashMap<>();
        private final String[] values = new String[256];
        private int next = 1;

        /** Returns the code for v, or -1 if the dictionary is full. */
        int code(String v) {
            if (v.isEmpty()) return 0;
            Integer c = codes.get(v);
            if (c != null) return c;
            synchronized (this) {
                c = codes.get(v);
                if (c != null) return c;
                if (next > 255) return -1;
                values[next] = v;
                codes.put(v, next);
                return next++;
            }
        }

        String value(int code) { return code == 0 ? "" : values[code]; }

        int size() { return next - 1; }
    }

    // ---- verbatim fallback: [1] f0|f1|...|fn ----

    private static byte[] verbatim(String[] fields) {
        StringBuilder sb = new StringBuilder(80);
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) sb.append('|');
            sb.append(fields[i]);
        }
        byte[] text = sb.toString().getBytes(StandardCharsets.UTF_8);
        byte[] b = new byte[text.length + 1];
        b[0] = VERBATIM;
        System.arraycopy(text, 0, b, 1, text.length);
        return b;
    }

    /** Splits b[off..end) on '|' into out; the last field keeps any extra '|'. */
    private static void split(byte[] b, int off, int end, String[] out, int firstField) {
        int field = firstField, start = off;
        for (int i = off; i < end && field < out.length - 1; i++) {
            if (b[i] == '|') {
                out[field++] = new String(b, start, i - start, StandardCharsets.UTF_8);
                start = i + 1;
            }
        }
        out[field++] = new String(b, start, end - start, StandardCharsets.UTF_8);
        while (field < out.length) out[field++] = "";
    }

    private static boolean isTenDigits(String s) {
        if (s.length() != 10) return false;
        for (int i = 0; i < 10; i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') return false;
        }
        return true;
    }

    // ---- providers: columns name(1), specialty(2), npi(3), network_status(6) ----

    public static final class ProviderCodec implements Codec {
        private static final String CLINIC = " CLINIC";
        private static final byte NAME_EMPTY = 0, NAME_PATTERN = 1, NAME_LITERAL = 2;
        final Dict specialty = new Dict(), network = new Dict();

        @Override public int fields() { return 4; }

        @Override public byte[] encode(String[] r) {
            String name = r[1], spec = r[2], npi = r[3], net = r[6];
            int sc = specialty.code(spec), nc = network.code(net);
            if (sc < 0 || nc < 0 || !isTenDigits(npi)) {
                return verbatim(new String[]{name, spec, npi, net});
            }
            byte mode;
            String stored;
            String suffix = " " + spec + CLINIC;
            if (name.isEmpty()) {
                mode = NAME_EMPTY;
                stored = "";
            } else if (!spec.isEmpty() && name.length() > suffix.length() && name.endsWith(suffix)) {
                mode = NAME_PATTERN;
                stored = name.substring(0, name.length() - suffix.length());
            } else {
                mode = NAME_LITERAL;
                stored = name;
            }
            byte[] nameBytes = stored.getBytes(StandardCharsets.UTF_8);
            long n = Long.parseLong(npi);
            byte[] b = new byte[9 + nameBytes.length];
            b[0] = COMPACT;
            b[1] = (byte) sc;
            b[2] = (byte) nc;
            b[3] = (byte) (n >>> 32);
            b[4] = (byte) (n >>> 24);
            b[5] = (byte) (n >>> 16);
            b[6] = (byte) (n >>> 8);
            b[7] = (byte) n;
            b[8] = mode;
            System.arraycopy(nameBytes, 0, b, 9, nameBytes.length);
            return b;
        }

        @Override public void decode(byte[] b, int off, int len, String[] out) {
            if (b[off] == VERBATIM) { split(b, off + 1, off + len, out, 0); return; }
            String spec = specialty.value(b[off + 1] & 0xFF);
            long n = ((long) (b[off + 3] & 0xFF) << 32) | ((long) (b[off + 4] & 0xFF) << 24)
                    | ((b[off + 5] & 0xFF) << 16) | ((b[off + 6] & 0xFF) << 8) | (b[off + 7] & 0xFF);
            String stored = new String(b, off + 9, len - 9, StandardCharsets.UTF_8);
            byte mode = b[off + 8];
            out[0] = mode == NAME_PATTERN ? stored + " " + spec + CLINIC : stored;
            out[1] = spec;
            String npi = Long.toString(n);
            out[2] = npi.length() == 10 ? npi : "0000000000".substring(npi.length()) + npi;
            out[3] = network.value(b[off + 2] & 0xFF);
        }
    }

    // ---- members: columns first(1), last(2), gender(3), plan_type(7), risk_score(10) ----

    public static final class MemberCodec implements Codec {
        final Dict gender = new Dict(), plan = new Dict();

        @Override public int fields() { return 5; }

        /** "3.29" -> 329; returns -1 unless it is digits with an optional '.' and <=2 decimals (and fits 15 bits). */
        static int hundredths(String s) {
            int n = s.length();
            if (n == 0 || n > 6) return -1;
            int dot = s.indexOf('.');
            if (dot == 0 || dot == n - 1) return -1;
            int intEnd = dot < 0 ? n : dot;
            int decimals = dot < 0 ? 0 : n - dot - 1;
            if (decimals > 2) return -1;
            int v = 0;
            for (int i = 0; i < n; i++) {
                if (i == dot) continue;
                char c = s.charAt(i);
                if (c < '0' || c > '9') return -1;
                v = v * 10 + (c - '0');
            }
            for (int i = decimals; i < 2; i++) v *= 10;
            return (intEnd > 0 && v <= 0x7FFF) ? v : -1;
        }

        @Override public byte[] encode(String[] r) {
            String first = r[1], last = r[2], g = r[3], p = r[7], risk = r[10];
            int gc = gender.code(g), pc = plan.code(p);
            int h = risk.isEmpty() ? 0xFFFF : hundredths(risk);
            if (gc < 0 || pc < 0 || h < 0 || first.indexOf('|') >= 0 || last.indexOf('|') >= 0) {
                return verbatim(new String[]{first, last, g, p, risk});
            }
            byte[] names = (first + '|' + last).getBytes(StandardCharsets.UTF_8);
            byte[] b = new byte[5 + names.length];
            b[0] = COMPACT;
            b[1] = (byte) gc;
            b[2] = (byte) pc;
            b[3] = (byte) (h >>> 8);
            b[4] = (byte) h;
            System.arraycopy(names, 0, b, 5, names.length);
            return b;
        }

        @Override public void decode(byte[] b, int off, int len, String[] out) {
            if (b[off] == VERBATIM) { split(b, off + 1, off + len, out, 0); return; }
            split(b, off + 5, off + len, out, 0);      // out[0]=first, out[1]=last
            out[2] = gender.value(b[off + 1] & 0xFF);
            out[3] = plan.value(b[off + 2] & 0xFF);
            int h = ((b[off + 3] & 0xFF) << 8) | (b[off + 4] & 0xFF);
            // Double.toString(329 / 100.0) == "3.29": the division is correctly rounded,
            // so it yields the same double the original text parses to.
            out[4] = h == 0xFFFF ? "" : Double.toString(h / 100.0);
        }
    }
}
