package org.oracle.com.ods.services.collectData.rawOds;

import java.io.*;
import java.sql.*;
import java.util.*;
import com.fasterxml.jackson.databind.ObjectMapper; // For JSON parsing
import org.oracle.com.ods.services.collectData.snowflake.SnowflakeQueryExecutor;
import org.oracle.com.ods.services.collectData.vertica.VerticaQueryExecutor;

public class RawOdsDatabaseEvidence {

    public static void main(String[] args) throws Exception {
        // Load configuration and tables
        Map<String, Object> snowflakeConfig = SnowflakeQueryExecutor.loadConfig();
        Map<String, Object> verticaConfig = VerticaQueryExecutor.loadConfig();
        List<Map<String, Object>> tables = loadTables();

        String outputDir = (String) snowflakeConfig.get("outputPath");

        // Snowflake and Vertica database connections
        try (Connection snowflakeConnection = SnowflakeQueryExecutor.connectToSnowflake(
                (String) snowflakeConfig.get("user"),
                (String) snowflakeConfig.get("password"),
                (String) snowflakeConfig.get("account"),
                (String) snowflakeConfig.get("warehouse"),
                (String) snowflakeConfig.get("database"));
             Connection verticaConnection = VerticaQueryExecutor.connectToVertica(
                     (String) verticaConfig.get("host"),
                     ((Double) verticaConfig.get("port")).intValue(),
                     (String) verticaConfig.get("database"),
                     (String) verticaConfig.get("user"),
                     (String) verticaConfig.get("password"))) {

            for (Map<String, Object> table : tables) {
                String tableName = (String) table.get("name");
                String primaryKey = (String) table.get("primaryKey");
                String snowflakeSchema = (String) table.get("snowflakeSchema");
                String verticaSchema = (String) table.get("verticaSchema");

                // Create individual folders for each table
                String snowflakeTableOutputDir = outputDir + "/snowflake/" + tableName;
                String verticaTableOutputDir = outputDir + "/vertica/" + tableName;

                new File(snowflakeTableOutputDir).mkdirs(); // Ensure Snowflake table folder exists
                new File(verticaTableOutputDir).mkdirs(); // Ensure Vertica table folder exists

                // Process for Snowflake
                System.out.println("Processing table in Snowflake: " + tableName);
                processTable(snowflakeConnection, snowflakeTableOutputDir, tableName, primaryKey, snowflakeSchema);

                // Process for Vertica
                System.out.println("Processing table in Vertica: " + tableName);
                processTable(verticaConnection, verticaTableOutputDir, tableName, primaryKey, verticaSchema);
            }
        }
    }


    private static List<Map<String, Object>> loadTables() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        try (InputStream is = new FileInputStream("src/main/java/org/oracle/com/ods/services/collectData/rawOds/tables.json")) {
            Map<String, Object> data = mapper.readValue(is, Map.class);
            return (List<Map<String, Object>>) data.get("tables");
        }
    }

    private static void processTable(Connection connection, String outputDir, String tableName, String primaryKey, String schema) throws SQLException, IOException {
        String sqlTableCount = "SELECT COUNT(*) AS COUNT FROM " + schema + "." + tableName;
        writeQueryResultToFile(connection, sqlTableCount, outputDir + "/" + tableName + "_COUNT.log");

        String sqlLoadErrorTableCount = "SELECT COUNT(*) AS PH_F_Load_Error FROM " + schema + ".PH_F_Load_Error WHERE object_id ILIKE '%" + tableName + "%'";
        writeQueryResultToFile(connection, sqlLoadErrorTableCount, outputDir + "/" + tableName + "_PH_F_Load_Error.log");

        String sqlZeroRow = "SELECT * FROM " + schema + "." + tableName + " WHERE " + primaryKey + " = 0";
        writeQueryResultToFile(connection, sqlZeroRow, outputDir + "/" + tableName + "_ZERO_ROW.log");

        String sqlNonZeroRow = "SELECT * FROM " + schema + "." + tableName + " WHERE " + primaryKey + " != 0 AND UPDT_CNT = 150 LIMIT 1";
        writeQueryResultToFile(connection, sqlNonZeroRow, outputDir + "/" + tableName + "_NON_ZERO_ROW.log");

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputDir + "/" + tableName + "_Queries.log", true))) {
            writer.write("Query Used For Count: " + sqlTableCount + "\n");
            writer.write("Query Used For Error Table: " + sqlLoadErrorTableCount + "\n");
            writer.write("Query Used For Zero Row: " + sqlZeroRow + "\n");
            writer.write("Query Used For Non Zero Row: " + sqlNonZeroRow + "\n");
        }
    }

    private static void writeQueryResultToFile(Connection connection, String query, String fileName) throws SQLException, IOException {
        try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(query);
             BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, true))) {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    writer.write("|" + metaData.getColumnName(i) + "\t\t\t|" + rs.getString(i) + "\t\t\t|\n");
                }
            }
        }
    }
}
