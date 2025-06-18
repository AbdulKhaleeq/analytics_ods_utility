package org.oracle.com.ods.services.collectData.vertica;

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

public class VerticaQueryExecutor {

    private static final Logger LOGGER = Logger.getLogger(VerticaQueryExecutor.class.getName());
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static Connection connectToVertica(String host, int port, String database, String user, String password) throws SQLException {
        String url = "jdbc:vertica://" + host + ":" + port + "/" + database;
        return DriverManager.getConnection(url, user, password);
    }

    public static Map<String, Object> loadConfig() throws IOException {
        Gson gson = new Gson();
        try (Reader reader = new FileReader("src/main/java/org/oracle/com/ods/services/collectData/vertica/config.json")) {
            return gson.fromJson(reader, new TypeToken<Map<String, Object>>() {}.getType());
        }
    }

    private static List<Map<String, Object>> loadTables() throws IOException {
        Gson gson = new Gson();
        try (Reader reader = new FileReader("src/main/java/org/oracle/com/ods/services/collectData/vertica/tables.json")) {
            Map<String, List<Map<String, Object>>> tables = gson.fromJson(reader, new TypeToken<Map<String, List<Map<String, Object>>>>() {}.getType());
            return tables.get("tables");
        }
    }

    private static void executeQueryAndWriteExcel(Connection connection, String dataQuery, String countQuery, String schemaName, String tableName, String outputFileName) {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(dataQuery)) {

            ResultSetMetaData rsmd = rs.getMetaData();

            Workbook workbook = new XSSFWorkbook();
            Sheet sheet = workbook.createSheet("Data");

            int rowNum = 0;
            Row metadataRow1 = sheet.createRow(rowNum++);
            metadataRow1.createCell(0).setCellValue("Schema Name: ");
            metadataRow1.createCell(1).setCellValue(schemaName);

            Row metadataRow2 = sheet.createRow(rowNum++);
            metadataRow2.createCell(0).setCellValue("Table Name: ");
            metadataRow2.createCell(1).setCellValue(tableName);

            Row metadataRow3 = sheet.createRow(rowNum++);
            metadataRow3.createCell(0).setCellValue("Query Executed: ");
            Cell queryCell = metadataRow3.createCell(1);
            queryCell.setCellValue(dataQuery);

            if (rsmd.getColumnCount() > 1) {
                int endColumn = rsmd.getColumnCount() - 1;
                if (endColumn > 1) {
                    sheet.addMergedRegion(new CellRangeAddress(metadataRow3.getRowNum(), metadataRow3.getRowNum(), 1, endColumn));
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
                    countRow.createCell(0).setCellValue("Total Row Count: ");
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

            try (Connection connection = connectToVertica(
                    (String) config.get("host"),
                    ((Double) config.get("port")).intValue(),
                    (String) config.get("database"),
                    (String) config.get("user"),
                    (String) config.get("password"))) {

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
                            System.out.println(defaultQuery);
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