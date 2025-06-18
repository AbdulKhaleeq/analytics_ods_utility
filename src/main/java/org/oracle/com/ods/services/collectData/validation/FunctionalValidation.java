package org.oracle.com.ods.services.collectData.validation;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.oracle.com.ods.db.PrimaryKeyExtractor;
import org.oracle.com.ods.services.collectData.vertica.VerticaQueryExecutor;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.oracle.com.ods.db.Database.getConnection;
import static org.oracle.com.ods.services.collectData.vertica.VerticaQueryExecutor.loadConfig;

public class FunctionalValidation {

    private static final Logger LOGGER = Logger.getLogger(FunctionalValidation.class.getName());
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("dd MMM yyyy");

    public static Connection connectToVertica(String host, int port, String database, String user, String password) throws SQLException {
        String url = "jdbc:vertica://" + host + ":" + port + "/" + database;
        return DriverManager.getConnection(url, user, password);
    }

    public static Connection connectToOracle(String host, int port, String database, String user, String password) throws SQLException {
        String url = "jdbc:oracle:thin:@" + host + ":" + port + ":" + database;
        return DriverManager.getConnection(url, user, password);
    }

    public static void executeQueryAndWriteExcel(Connection verticaConnection, Connection oracleConnection, String tableName, String outputDir, String associateId) {
        Set<String> primaryKeys = PrimaryKeyExtractor.extractPrimaryKeys(tableName);

        String orderByClause = "";
        if (!primaryKeys.isEmpty()) {
            orderByClause = " ORDER BY " + String.join(", ", primaryKeys);
        }

        String verticaSchema = "APP_CMTDEV";
        String oracleSchema = "V500";
        String verticaQuery = "SELECT * FROM " + verticaSchema + "." + tableName + " WHERE UPDT_CNT=150" + orderByClause;
        String oracleQuery = "SELECT * FROM " + oracleSchema + "." + tableName + " WHERE UPDT_CNT=150" + orderByClause;
        String countQuery = "SELECT COUNT(*) FROM ";

        String outputFileName = outputDir + tableName + "_validation_output.xlsx";

        try (Statement verticaStmt = verticaConnection.createStatement();
             Statement oracleStmt = oracleConnection.createStatement();
             ResultSet verticaRs = verticaStmt.executeQuery(verticaQuery);
             ResultSet oracleRs = oracleStmt.executeQuery(oracleQuery)) {

            ResultSetMetaData verticaRsmd = verticaRs.getMetaData();
            ResultSetMetaData oracleRsmd = oracleRs.getMetaData();

            Workbook workbook = new XSSFWorkbook();
            Sheet sheet = workbook.createSheet("Validation");

            // Add test header metadata
            addTestMetadata(sheet, tableName, associateId);

            int rowNum = 8;

            // Add Vertica Metadata and Data
            rowNum = addDBMetadataAndData(sheet, rowNum, "Vertica", verticaSchema, tableName, verticaQuery, verticaRs, verticaRsmd);

            rowNum += 3; // Add space before Oracle data

            // Add Oracle Metadata and Data
            rowNum = addDBMetadataAndData(sheet, rowNum, "Oracle", oracleSchema, tableName, oracleQuery, oracleRs, oracleRsmd);

            // Add Count Comparison
            rowNum += 3;
            addCountComparison(sheet, rowNum, verticaStmt, oracleStmt, countQuery, verticaSchema, oracleSchema, tableName);

            for (int i = 0; i < verticaRsmd.getColumnCount(); i++) {
                sheet.autoSizeColumn(i);
            }

            // Write to Excel
            try (FileOutputStream fileOut = new FileOutputStream(outputFileName)) {
                workbook.write(fileOut);
            }

            workbook.close();

        } catch (SQLException | IOException e) {
            LOGGER.log(Level.SEVERE, "Error executing query or writing Excel file", e);
        }
    }

    private static void addTestMetadata(Sheet sheet, String tableName, String associateId) {
        Row row1 = sheet.createRow(0);
        row1.createCell(0).setCellValue("Issue: ODS Mappings for " + tableName);

        Row row2 = sheet.createRow(1);
        row2.createCell(0).setCellValue("Solution: Operational Data Store – Crawlers");

        Row row3 = sheet.createRow(2);
        row3.createCell(0).setCellValue("Test Date: " + DATE_FORMATTER.format(new Date()));

        Row row4 = sheet.createRow(3);
        row4.createCell(0).setCellValue("Environment: Dev");

        Row row5 = sheet.createRow(4);
        row5.createCell(0).setCellValue("Operating System: Mac");

        Row row6 = sheet.createRow(5);
        row6.createCell(0).setCellValue("Associate ID: " + associateId);
    }

    private static int addDBMetadataAndData(Sheet sheet, int rowNum, String dbName, String schemaName, String tableName, String query, ResultSet rs, ResultSetMetaData rsmd) throws SQLException {
        // Add DB Metadata
        Row dbMetadataRow1 = sheet.createRow(rowNum++);
        dbMetadataRow1.createCell(0).setCellValue("DB: ");
        dbMetadataRow1.createCell(1).setCellValue(dbName);

        Row dbMetadataRow2 = sheet.createRow(rowNum++);
        dbMetadataRow2.createCell(0).setCellValue("Schema Name: ");
        dbMetadataRow2.createCell(1).setCellValue(schemaName);

        Row dbMetadataRow3 = sheet.createRow(rowNum++);
        dbMetadataRow3.createCell(0).setCellValue("Table Name: ");
        dbMetadataRow3.createCell(1).setCellValue(tableName);

        Row dbMetadataRow4 = sheet.createRow(rowNum++);
        dbMetadataRow4.createCell(0).setCellValue("Query Executed: ");
        dbMetadataRow4.createCell(1).setCellValue(query);

        sheet.addMergedRegion(new CellRangeAddress(dbMetadataRow4.getRowNum(), dbMetadataRow4.getRowNum(), 1, rsmd.getColumnCount() - 1));

        // Add Table Headers
        Row headerRow = sheet.createRow(rowNum++);
        int columnCount = rsmd.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            headerRow.createCell(i - 1).setCellValue(rsmd.getColumnName(i));
        }

        // Add Data
        while (rs.next()) {
            Row row = sheet.createRow(rowNum++);
            for (int i = 1; i <= columnCount; i++) {
                Cell cell = row.createCell(i - 1);
                Object value = rs.getObject(i);
                if (value != null) {
                    cell.setCellValue(value.toString());
                } else {
                    cell.setCellValue("null");
                }
            }
        }

        return rowNum;
    }

    private static void addCountComparison(Sheet sheet, int rowNum, Statement verticaStmt, Statement oracleStmt, String countQuery, String verticaSchema, String oracleSchema, String tableName) throws SQLException {
        // Vertica count
        ResultSet verticaCountRs = verticaStmt.executeQuery(countQuery + verticaSchema + "." + tableName);
        verticaCountRs.next();
        int verticaCount = verticaCountRs.getInt(1);

        // Oracle count
        ResultSet oracleCountRs = oracleStmt.executeQuery(countQuery + oracleSchema + "." + tableName);
        oracleCountRs.next();
        int oracleCount = oracleCountRs.getInt(1);

        // Add count comparison header row (Row Count (Vertica), Row Count (Oracle), Difference)
        Row countComparisonHeaderRow = sheet.createRow(rowNum);
        countComparisonHeaderRow.createCell(0).setCellValue("Row Count (Vertica):");
        countComparisonHeaderRow.createCell(1).setCellValue("Row Count (Oracle):");
        countComparisonHeaderRow.createCell(2).setCellValue("Difference:");

        // Add count values row (with Vertica count, Oracle count, and Difference)
        Row countComparisonValuesRow = sheet.createRow(rowNum + 1);
        countComparisonValuesRow.createCell(0).setCellValue(verticaCount); // Vertica count
        countComparisonValuesRow.createCell(1).setCellValue(oracleCount);  // Oracle count
        countComparisonValuesRow.createCell(2).setCellValue(Math.abs(verticaCount - oracleCount));
    }

    public static void main(String[] args) {
        try {
            // Vertica DB Connection
            Map<String, Object> config = loadConfig();
            Connection verticaConnection = VerticaQueryExecutor.connectToVertica(
                    (String) config.get("host"),
                    ((Double) config.get("port")).intValue(),
                    (String) config.get("database"),
                    (String) config.get("user"),
                    (String) config.get("password"));

            // Oracle DB Connection
            Connection oracleConnection = getConnection();

            Scanner scanner = new Scanner(System.in);
            System.out.print("Please provide the output directory path: ");
            String outputDir = scanner.nextLine();
            if (!outputDir.endsWith(File.separator)) {
                outputDir += File.separator;
            }

            System.out.print("Please provide a comma-separated list of table names: ");
            String tableNames = scanner.nextLine();
            String[] tables = tableNames.split(",");

            System.out.print("Please provide the Associate Id: ");
            String associateId = scanner.nextLine();

            for (String table : tables) {
                executeQueryAndWriteExcel(verticaConnection, oracleConnection, table.trim(), outputDir, associateId);
            }

            verticaConnection.close();
            oracleConnection.close();

        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Database connection error", e);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error loading configuration or tables", e);
        }
    }
}

