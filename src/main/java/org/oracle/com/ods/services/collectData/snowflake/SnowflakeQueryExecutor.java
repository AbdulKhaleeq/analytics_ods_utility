package org.oracle.com.ods.services.collectData.snowflake;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SnowflakeQueryExecutor {

    private static final Logger LOGGER = Logger.getLogger(SnowflakeQueryExecutor.class.getName());
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static Connection connectToSnowflake(String user, String password, String account, String warehouse, String database) throws SQLException {
        String url = "jdbc:snowflake://" + account + ".snowflakecomputing.com";
        Properties props = new Properties();
        props.put("user", user);
        props.put("password", password);
        props.put("warehouse", warehouse);
        props.put("db", database);
        return DriverManager.getConnection(url, props);
    }

    public static Map<String, Object> loadConfig() throws IOException {
        Gson gson = new Gson();
        try (Reader reader = new FileReader("src/main/java/org/oracle/com/ods/services/collectData/snowflake/config.json")) {
            return gson.fromJson(reader, new TypeToken<Map<String, Object>>() {}.getType());
        }
    }

    private static List<Map<String, Object>> loadTables() throws IOException {
        Gson gson = new Gson();
        try (Reader reader = new FileReader("src/main/java/org/oracle/com/ods/services/collectData/snowflake/tables.json")) {
            Map<String, List<Map<String, Object>>> tables = gson.fromJson(reader, new TypeToken<Map<String, List<Map<String, Object>>>>() {}.getType());
            return tables.get("tables");
        }
    }

    private static void executeQueryAndWriteExcel(Connection connection, String dataQuery, String countQuery, String schemaName, String tableName, String outputFileName) {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(dataQuery)) {

            ResultSetMetaData rsmd = rs.getMetaData();

            Workbook workbook = new XSSFWorkbook();
            Sheet sheet = workbook.createSheet("Results");

            int rowNum = 0;
            Row metaDataRow1 = sheet.createRow(rowNum++);
            metaDataRow1.createCell(0).setCellValue("Schema Name:");
            metaDataRow1.createCell(1).setCellValue(schemaName);

            Row metaDataRow2 = sheet.createRow(rowNum++);
            metaDataRow2.createCell(0).setCellValue("Table Name:");
            metaDataRow2.createCell(1).setCellValue(tableName);

            Row metaDataRow3 = sheet.createRow(rowNum++);
            metaDataRow3.createCell(0).setCellValue("Query Executed:");
            Cell queryCell = metaDataRow3.createCell(1);
            queryCell.setCellValue(dataQuery);

            if (rsmd.getColumnCount() > 1) {
                int endColumn = rsmd.getColumnCount() - 1;
                if (endColumn > 1) {
                    sheet.addMergedRegion(new CellRangeAddress(metaDataRow3.getRowNum(), metaDataRow3.getRowNum(), 1, endColumn));
                } else {
                    LOGGER.warning("Skipping merged region creation: Not enough columns to merge.");
                }
            }


            rowNum += 2;

            Row headerRow = sheet.createRow(rowNum++);
            int columnCount = rsmd.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                headerRow.createCell(i - 1).setCellValue(rsmd.getColumnName(i));
            }

            while (rs.next()) {
                Row row = sheet.createRow(rowNum++);
                for (int i = 1; i <= columnCount; i++) {
                    Cell cell = row.createCell(i - 1);
                    int columnType = rsmd.getColumnType(i);

                    if (columnType == Types.TIMESTAMP || columnType == Types.DATE) {
                        Timestamp timestamp = rs.getTimestamp(i);
                        if (timestamp != null) {
                            cell.setCellValue(DATE_FORMATTER.format(timestamp));
                        } else {
                            cell.setCellValue("null");
                        }
                    } else {
                        String value = rs.getString(i);
                        if (value != null) {
                            cell.setCellValue(value);
                        } else {
                            cell.setCellValue("null");
                        }
                    }
                }
            }

            rowNum += 2;

            if (countQuery != null) {
                ResultSet countRs = stmt.executeQuery(countQuery);
                if (countRs.next()) {
                    Row countRow = sheet.createRow(rowNum++);
                    countRow.createCell(0).setCellValue("Total Row Count:");
                    countRow.createCell(1).setCellValue(countRs.getString(1));
                }
            }

            for (int i = 0; i < columnCount; i++) {
                sheet.autoSizeColumn(i);
            }

            try (FileOutputStream fileOut = new FileOutputStream(outputFileName)) {
                workbook.write(fileOut);
            }

            workbook.close();

        } catch (SQLException | IOException e) {
            LOGGER.log(Level.SEVERE, "Error executing query or writing Excel file", e);
        }
    }

    public static void main(String[] args) {
        try {
            Map<String, Object> config = loadConfig();
            List<Map<String, Object>> tables = loadTables();

            try (Connection connection = connectToSnowflake(
                    (String) config.get("user"),
                    (String) config.get("password"),
                    (String) config.get("account"),
                    (String) config.get("warehouse"),
                    (String) config.get("database"))) {

                Scanner scanner = new Scanner(System.in);
                System.out.print("Please provide the output directory path: ");
                String outputDir = scanner.nextLine();
                if (!outputDir.endsWith(File.separator)) {
                    outputDir += File.separator;
                }

                System.out.print("Do you want to execute a custom query? (yes/no): ");
                String customQueryOption = scanner.nextLine();

                if (customQueryOption.equalsIgnoreCase("yes")) {
                    System.out.print("Enter your custom query: ");
                    String customQuery = scanner.nextLine();
                    System.out.print("Enter the schema name for the custom query: ");
                    String schemaName = scanner.nextLine();
                    System.out.print("Enter the table name for the custom query: ");
                    String tableName = scanner.nextLine();
                    String outputFileName = outputDir + schemaName + "_" + tableName + "_custom_query_output.xlsx";
                    executeQueryAndWriteExcel(connection, customQuery, null, schemaName, tableName, outputFileName);
                } else {
                    // Default Query Execution
                    for (Map<String, Object> table : tables) {
                        String tableName = (String) table.get("name");
                        String columns = (String) table.get("columns");
                        List<String> schemas = (List<String>) table.get("schemas");

                        for (String schema : schemas) {
                            String defaultQuery = "SELECT " + columns + " FROM " + schema + "." + tableName + " WHERE UPDT_CNT=150";

                            String countQuery = "SELECT COUNT(*) FROM " + schema + "." + tableName;

                            String outputFileName = outputDir + schema + "_" + tableName + "_output.xlsx";
                            executeQueryAndWriteExcel(connection, defaultQuery, countQuery, schema, tableName, outputFileName);
                        }
                    }
                }

            } catch (SQLException e) {
                LOGGER.log(Level.SEVERE, "Database connection error", e);
            }

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error loading configuration or tables", e);
        }
    }
}
