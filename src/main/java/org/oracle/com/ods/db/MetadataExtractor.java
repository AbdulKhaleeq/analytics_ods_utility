package org.oracle.com.ods.db;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataExtractor {

    private static final Logger logger = LoggerFactory.getLogger(MetadataExtractor.class);

    public MetadataExtractor() {
    }

    /**
     * Extracts metadata based on whether it is for new tables or enhancements.
     *
     * @param tableName Input table name.
     * @param avdlFields List of avdl fields (for new tables), can be null or empty for enhancements.
     * @param newFields List of new fields (for enhancements), can be null or empty for new tables.
     * @return metadataList List of metadata extracted from db for the table or fields.
     */
    public static List<Map<String, Object>> extractMetadata(String tableName, List<String> avdlFields, List<String> newFields) {
        if (newFields != null && !newFields.isEmpty()) {
            return extractFieldMetadata(tableName, newFields);
        } else {
            return extractTableMetadata(tableName, avdlFields);
        }
    }

    private static List<Map<String, Object>> extractTableMetadata(String tableName, List<String> avdlFields) {
        List<Map<String, Object>> metadataList = new ArrayList<>();
        try (Connection conn = Database.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT COLUMN_NAME, DATA_TYPE, NULLABLE, DATA_DEFAULT, DATA_LENGTH " +
                             "FROM USER_TAB_COLUMNS WHERE TABLE_NAME = ? ORDER BY COLUMN_NAME")) {

            stmt.setString(1, tableName.toUpperCase());
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    if (avdlFields.contains(columnName.toLowerCase())) {
                        Map<String, Object> columnData = new HashMap<>();
                        columnData.put("COLUMN_NAME", columnName);
                        columnData.put("DATA_TYPE", rs.getString("DATA_TYPE"));

                        boolean isNullable = "Y".equalsIgnoreCase(rs.getString("NULLABLE"));
                        columnData.put("NULLABLE", isNullable);

                        String dataDefault = rs.getString("DATA_DEFAULT");
                        columnData.put("DATA_DEFAULT", processDefaultValue(rs.getString("DATA_TYPE"), dataDefault));
                        columnData.put("DATA_LENGTH", rs.getInt("DATA_LENGTH"));
                        metadataList.add(columnData);
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("Error extracting metadata for table: " + tableName, e);
        }
        return metadataList;
    }

    private static List<Map<String, Object>> extractFieldMetadata(String tableName, List<String> fields) {
        List<Map<String, Object>> metadataList = new ArrayList<>();
        try (Connection conn = Database.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT COLUMN_NAME, DATA_TYPE, NULLABLE, DATA_DEFAULT, DATA_LENGTH " +
                             "FROM USER_TAB_COLUMNS WHERE TABLE_NAME = ? AND COLUMN_NAME IN (" + generatePlaceholders(fields.size()) + ") ORDER BY COLUMN_NAME")) {

            stmt.setString(1, tableName.toUpperCase());
            for (int i = 0; i < fields.size(); i++) {
                stmt.setString(i + 2, fields.get(i).toUpperCase());
            }

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    Map<String, Object> columnData = new HashMap<>();
                    columnData.put("COLUMN_NAME", columnName);
                    columnData.put("DATA_TYPE", rs.getString("DATA_TYPE"));

                    boolean isNullable = "Y".equalsIgnoreCase(rs.getString("NULLABLE"));
                    columnData.put("NULLABLE", isNullable);

                    String dataDefault = rs.getString("DATA_DEFAULT");
                    columnData.put("DATA_DEFAULT", processDefaultValue(rs.getString("DATA_TYPE"), dataDefault));
                    columnData.put("DATA_LENGTH", rs.getInt("DATA_LENGTH"));
                    metadataList.add(columnData);
                }
            }
        } catch (SQLException e) {
            logger.error("Error extracting metadata for table: " + tableName, e);
        }
        return metadataList;
    }

    private static String processDefaultValue(String dataType, String defaultValue) {
        if (defaultValue == null || defaultValue.trim().isEmpty()) {
            return null;
        }
        String trimmedValue = defaultValue.trim();

        if ((dataType.equalsIgnoreCase("DATE") || dataType.equalsIgnoreCase("TIMESTAMP") || dataType.equalsIgnoreCase("TIMESTAMP(9)")) && (trimmedValue.equalsIgnoreCase("SYSDATE") || trimmedValue.equalsIgnoreCase("SYS_EXTRACT_UTC(SYSTIMESTAMP)"))) {
            return null;
        }

        if ((dataType.equalsIgnoreCase("DATE") || dataType.equalsIgnoreCase("TIMESTAMP")) && trimmedValue.startsWith("TO_DATE(")) {
            return parseToDate(trimmedValue);
        }

        return trimmedValue.equals("' '") ? " " : trimmedValue;
    }

    private static String parseToDate(String toDateString) {
        // Updated pattern to handle optional spaces around the comma
        Pattern pattern = Pattern.compile("TO_DATE\\s*\\(\\s*'([^']*)'\\s*,\\s*'[^']*'\\s*\\)");
        Matcher matcher = pattern.matcher(toDateString);
        if (matcher.find()) {
            String dateString = matcher.group(1);
            // Updated input format to match "HH24" as "HH" in Java
            SimpleDateFormat inputFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
            SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd");
            try {
                return outputFormat.format(inputFormat.parse(dateString));
            } catch (ParseException e) {
                logger.error("Error parsing date: " + dateString, e);
            }
        }
        return null;
    }

    private static String generatePlaceholders(int count) {
        StringBuilder placeholders = new StringBuilder();
        for (int i = 0; i < count; i++) {
            placeholders.append("?");
            if (i < count - 1) {
                placeholders.append(",");
            }
        }
        return placeholders.toString();
    }
}