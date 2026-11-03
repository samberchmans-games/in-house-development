package com.example.claims;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/** Cleansing rules for the dirty claims feed. */
public final class Cleanse {

    private Cleanse() {}

    private static final Map<String, String> STATE_NAME_TO_CODE = new HashMap<>();
    static {
        STATE_NAME_TO_CODE.put("TEXAS", "TX");
        STATE_NAME_TO_CODE.put("CALIFORNIA", "CA");
        STATE_NAME_TO_CODE.put("FLORIDA", "FL");
        STATE_NAME_TO_CODE.put("NEW YORK", "NY");
        STATE_NAME_TO_CODE.put("ILLINOIS", "IL");
        STATE_NAME_TO_CODE.put("PENNSYLVANIA", "PA");
        STATE_NAME_TO_CODE.put("OHIO", "OH");
        STATE_NAME_TO_CODE.put("GEORGIA", "GA");
        STATE_NAME_TO_CODE.put("NORTH CAROLINA", "NC");
        STATE_NAME_TO_CODE.put("MICHIGAN", "MI");
    }

    /** Trim + uppercase; null/empty/UNKNOWN/N/A -> null. */
    public static String id(String raw) {
        if (raw == null) return null;
        String v = raw.trim().toUpperCase(Locale.ROOT);
        if (v.isEmpty() || v.equals("UNKNOWN") || v.equals("N/A")) return null;
        return v;
    }

    /** Normalize claim_status variants to PAID / DENIED / PENDING; unknown -> null. */
    public static String status(String raw) {
        if (raw == null) return null;
        String v = raw.trim().toUpperCase(Locale.ROOT);
        switch (v) {
            case "PAID": case "PD": return "PAID";
            case "DENIED": case "D": return "DENIED";
            case "PENDING": case "P": return "PENDING";
            default: return null;
        }
    }

    /** Normalize state: full names and lowercase -> 2-letter code; invalid -> null. */
    public static String state(String raw) {
        if (raw == null) return null;
        String v = raw.trim().toUpperCase(Locale.ROOT);
        if (v.length() > 2) v = STATE_NAME_TO_CODE.getOrDefault(v, null);
        if (v == null || v.length() != 2 || !Character.isLetter(v.charAt(0)) || !Character.isLetter(v.charAt(1))) return null;
        if (v.equals("XX")) return null;
        return v;
    }

    /** Parse billed_amount text: strips $ and commas; empty/invalid -> null. Keeps negatives (flagged downstream). */
    public static Double amount(String raw) {
        if (raw == null) return null;
        String v = raw.trim().replace("$", "").replace(",", "");
        if (v.isEmpty()) return null;
        try {
            return Double.parseDouble(v);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** Accept only ISO yyyy-MM-dd dates that are valid; everything else -> null. */
    public static String isoDate(String raw) {
        if (raw == null) return null;
        String v = raw.trim();
        if (v.length() == 10 && v.charAt(4) == '-' && v.charAt(7) == '-') {
            try {
                int y = Integer.parseInt(v.substring(0, 4));
                int m = Integer.parseInt(v.substring(5, 7));
                int d = Integer.parseInt(v.substring(8, 10));
                if (y >= 1900 && y <= 2100 && m >= 1 && m <= 12 && d >= 1 && d <= 31) return v;
            } catch (NumberFormatException ignored) {}
        }
        return null;
    }

    /** Uppercase trimmed code; XXXXX/9999/empty -> null. */
    public static String code(String raw) {
        if (raw == null) return null;
        String v = raw.trim().toUpperCase(Locale.ROOT);
        if (v.isEmpty() || v.equals("XXXXX") || v.equals("9999")) return null;
        return v;
    }
}
