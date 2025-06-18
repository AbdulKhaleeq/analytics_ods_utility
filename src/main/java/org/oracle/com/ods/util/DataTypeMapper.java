package org.oracle.com.ods.util;

/**
 * Utility class for mapping database data types to target system data types.
 */
public class DataTypeMapper {

    private DataTypeMapper() {
        // Private constructor to prevent instantiation
    }

    /**
     * Maps an Oracle database type to a target system type.
     *
     * @param oracleType The Oracle database type.
     * @return The mapped target system type.
     */
    public static String mapDataType(String oracleType) {
        switch (oracleType) {
            case "VARCHAR2":
            case "CHAR":
                return "STRING";
            case "NUMBER":
            case "INTEGER":
                return "LONG";
            case "DATE":
            case "TIMESTAMP":
            case "TIMESTAMP(9)":
                return "TIMESTAMP";
            case "FLOAT":
            case "DOUBLE":
                return "DOUBLE";
            case "CLOB":
                return "STRING";
            default:
                throw new RuntimeException("Unsupported oracle type: " + oracleType);
        }
    }

    /**
     * Maps an Oracle database type to snowflake database system type.
     *
     * @param dbDataType The Oracle database type.
     * @return The mapped vertica system type.
     */
    public static String mapToSnowflakeDataType(String dbDataType, int length) {
        switch (dbDataType.toUpperCase()) {
            case "NUMBER":
            case "INTEGER":
            case "LONG":
                return "INT";
            case "VARCHAR":
            case "VARCHAR2":
                return "VARCHAR(" + (2 * length) + ")";
            case "CHAR":
                return "CHAR(" + (2 * length) + ")";
            case "FLOAT":
            case "DOUBLE":
                return "FLOAT";
            case "TIMESTAMP":
            case "TIMESTAMP(9)":
            case "DATE":
                return "TIMESTAMP_LTZ";
            case "CLOB":
                return "VARCHAR(65000)";
            default:
                throw new RuntimeException("Unsupported oracle type: " + dbDataType);
        }
    }

    /**
     * Maps an Millennium Oracle database type to Oracle ADW database system type.
     *
     * @param dbDataType The Oracle database type.
     * @return The mapped vertica system type.
     */
    public static String mapToOracleADWDataType(String dbDataType, int length) {
        switch (dbDataType.toUpperCase()) {
            case "NUMBER":
            case "INTEGER":
            case "LONG":
                return "NUMBER";
            case "VARCHAR":
            case "VARCHAR2":
                return "VARCHAR2(" + (2 * length) + ")";
            case "CHAR":
                return "CHAR(" + (2 * length) + ")";
            case "FLOAT":
            case "DOUBLE":
                return "FLOAT";
            case "TIMESTAMP":
            case "TIMESTAMP(9)":
            case "DATE":
                return "TIMESTAMP(9)";
            case "CLOB":
                return "CLOB";
            default:
                throw new RuntimeException("Unsupported oracle type: " + dbDataType);
        }
    }

    /**
     * Maps an Oracle database type to vertica database system type.
     *
     * @param dbDataType The Oracle database type.
     * @return The mapped snowflake system type.
     */
    public static String mapToVerticaDataType(String dbDataType, int length) {
        switch (dbDataType.toUpperCase()) {
            case "INT":
            case "INTEGER":
            case "LONG":
            case "NUMBER":
                return "INTEGER";
            case "VARCHAR":
            case "VARCHAR2":
                return "VARCHAR(" + (2 * length) + ")";
            case "CHAR":
                return "CHAR(" + (2 * length) + ")";
            case "FLOAT":
            case "DOUBLE":
                return "FLOAT";
            case "TIMESTAMP":
            case "TIMESTAMP(9)":
            case "DATE":
                return "TIMESTAMPTZ";
            case "CLOB":
                return "VARCHAR(65000)";
            default:
                throw new RuntimeException("Unsupported oracle type: " + dbDataType);
        }
    }
}
