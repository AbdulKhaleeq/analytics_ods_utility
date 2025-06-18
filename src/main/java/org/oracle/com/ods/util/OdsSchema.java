package org.oracle.com.ods.util;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class OdsSchema {

    /**
     * Encodes a schema string to a Base64 encoded string.
     *
     * @param schema The schema string to encode.
     * @return The Base64 encoded string.
     */
    public static String getEncodedString(String schema) {
        byte[] byteArr = schema.getBytes(StandardCharsets.UTF_8);
        return Base64.getEncoder().encodeToString(byteArr);
    }

    /**
     * Cleans a string by removing unnecessary whitespace while retaining escape characters.
     *
     * @param input The input string to clean.
     * @return The cleaned string.
     */
    public static String cleanString(String input) {
        // Remove spaces, tabs, and newlines but retain escape characters (\)
        String cleaned = input.replaceAll("[ \\t\\n\\r]+", "").replaceAll("(?<!\\\\)\\\\(?!\\\\)", "");
        return cleaned;
    }
}
