package org.oracle.com.ods.util;

import org.oracle.com.ods.util.DataTypeMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class MigrationGenerator {

    private static final Logger logger = LoggerFactory.getLogger(MigrationGenerator.class);
    private static final String MIGRATION_ID_TEMPLATE = "migration_id=%d,PHANALYTIC-(replace jira number):%s %s\n\n";
    private static final String MIGRATION_ID_TEMPLATE_FOR_ADW = "migration_id=%d, %s %s\n\n";
    private static final String ADD_ROW_VERSION_COLUMN = "    _ROW_VERSION INT DEFAULT 0 NOT NULL,\n";
    private static final String ADW_ROW_VERSION_COLUMN = "    ROW_VERSION NUMBER DEFAULT 0 NOT NULL,\n";
    private static final String ADD_UPDATE_DT_COLUMN = "    ODS_UPDATE_DT_TM TIMESTAMPTZ DEFAULT SYSDATE() NOT NULL\n";
    private static final String ODS_UPDATE_DT_COLUMN_FOR_ADW = "    ODS_UPDATE_DT_TM TIMESTAMP(9) DEFAULT SYSDATE \n";
    private static final String GRANT_STATEMENT_TEMPLATE = "GRANT SELECT ON  %s TO ${schema}_reader;\n\n";

    /**
     * Generates Snowflake DDL for the specified table.
     *
     * @param metadata   List of column metadata.
     * @param tableName  The name of the table.
     * @return The generated Snowflake DDL statement.
     */
    public String generateSnowflakeDDL(List<Map<String, Object>> metadata, String tableName) {
        StringBuilder ddl = new StringBuilder();

        ddl.append(String.format(MIGRATION_ID_TEMPLATE, 1, "Adding new Snowflake table", tableName))
                .append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (\n");

        for (Map<String, Object> column : metadata) {
            String columnName = (String) column.get("COLUMN_NAME");
            String dataType = (String) column.get("DATA_TYPE");
            boolean nullable = (boolean) column.get("NULLABLE");
            int length = (int) column.get("DATA_LENGTH");

            ddl.append("    ").append(columnName).append(" ")
                    .append(DataTypeMapper.mapToSnowflakeDataType(dataType, length));
            if (!nullable) {
                ddl.append(" NOT NULL");
            }
            ddl.append(",\n");
        }

        ddl.append(ADD_ROW_VERSION_COLUMN)
                .append(ADD_UPDATE_DT_COLUMN)
                .append(");\n\n")
                .append(String.format(GRANT_STATEMENT_TEMPLATE, tableName));

        logger.info("Generated Snowflake DDL for table: {}", tableName);
        return ddl.toString();
    }

    /**
     * Generates ADW DDL for the specified table.
     *
     * @param metadata   List of column metadata.
     * @param tableName  The name of the table.
     * @return The generated ADW DDL statement.
     */
    public String generateAdwDDL(List<Map<String, Object>> metadata, String tableName) {
        StringBuilder ddl = new StringBuilder();

        ddl.append(String.format(MIGRATION_ID_TEMPLATE_FOR_ADW, 1, "Adding new Oracle ADW table", tableName))
                .append("CREATE TABLE ").append(tableName).append(" (\n");

        for (Map<String, Object> column : metadata) {
            String columnName = (String) column.get("COLUMN_NAME");
            String dataType = (String) column.get("DATA_TYPE");
            boolean nullable = (boolean) column.get("NULLABLE");
            int length = (int) column.get("DATA_LENGTH");

            ddl.append("    ").append(columnName).append(" ")
                    .append(DataTypeMapper.mapToOracleADWDataType(dataType, length));
            if (!nullable) {
                ddl.append(" NOT NULL");
            }
            ddl.append(",\n");
        }

        ddl.append(ADW_ROW_VERSION_COLUMN)
                .append(ODS_UPDATE_DT_COLUMN_FOR_ADW)
                .append(");\n\n")
                .append(String.format(GRANT_STATEMENT_TEMPLATE, tableName));

        logger.info("Generated Oracle ADW DDL for table: {}", tableName);
        return ddl.toString();
    }

    /**
     * Generates Vertica DDL for the specified table.
     *
     * @param metadata   List of column metadata.
     * @param tableName  The name of the table.
     * @param primaryKeys Set of primary keys for the table.
     * @return The generated Vertica DDL statement.
     */
    public String generateVerticaDDL(List<Map<String, Object>> metadata, String tableName, Set<String> primaryKeys) {
        StringBuilder ddl = new StringBuilder();

        ddl.append(String.format(MIGRATION_ID_TEMPLATE, 1, "Adding new Vertica table", tableName))
                .append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (\n");

        for (Map<String, Object> column : metadata) {
            String columnName = (String) column.get("COLUMN_NAME");
            String dataType = (String) column.get("DATA_TYPE");
            boolean nullable = (boolean) column.get("NULLABLE");
            int length = (int) column.get("DATA_LENGTH");

            ddl.append("    ").append(columnName).append(" ")
                    .append(DataTypeMapper.mapToVerticaDataType(dataType, length));
            if (!nullable) {
                ddl.append(" NOT NULL");
            }
            ddl.append(",\n");
        }

        ddl.append("    _ROW_VERSION int NOT NULL DEFAULT 0,\n")
                .append("    ODS_UPDATE_DT_TM TIMESTAMPTZ DEFAULT SYSDATE() NOT NULL\n")
                .append(");\n")
                .append("\nCREATE PROJECTION IF NOT EXISTS ").append(tableName).append("_SUPER (\n");

        for (Map<String, Object> column : metadata) {
            ddl.append("    ").append(column.get("COLUMN_NAME")).append(",\n");
        }
        ddl.append("    _ROW_VERSION,\n")
                .append("    ODS_UPDATE_DT_TM\n")
                .append(") AS SELECT \n");

        for (Map<String, Object> column : metadata) {
            ddl.append("    ").append(column.get("COLUMN_NAME")).append(",\n");
        }
        ddl.append("    _ROW_VERSION,\n")
                .append("    ODS_UPDATE_DT_TM\n")
                .append(" FROM ").append(tableName)
                .append("\n ORDER BY \n");

        if (!primaryKeys.isEmpty()) {
            for (String pk : primaryKeys) {
                ddl.append("    ").append(pk).append(",\n");
            }
            ddl.setLength(ddl.length() - 2); // Remove last comma and space
        } else {
            ddl.append("   replace");
        }

        ddl.append("\n SEGMENTED BY HASH(");

        if (!primaryKeys.isEmpty()) {
            for (String pk : primaryKeys) {
                ddl.append(pk).append(", ");
                break;
            }
            ddl.setLength(ddl.length() - 2); // Remove last comma and space
        } else {
            ddl.append("replace");
        }

        ddl.append(") ALL NODES KSAFE 1;\n\n")
                .append(String.format(GRANT_STATEMENT_TEMPLATE, tableName))
                .append(String.format(MIGRATION_ID_TEMPLATE, 2, "Adding new Vertica temp table", tableName + "_TEMP"))
                .append("SET SEARCH_PATH TO public;\n\n")
                .append("CREATE GLOBAL TEMPORARY TABLE IF NOT EXISTS ").append(tableName).append("_TEMP (\n");

        for (Map<String, Object> column : metadata) {
            ddl.append("    ").append(column.get("COLUMN_NAME")).append(" ")
                    .append(DataTypeMapper.mapToVerticaDataType(column.get("DATA_TYPE").toString(), (int) column.get("DATA_LENGTH"))).append(",\n");
        }

        ddl.append("    _ROW_VERSION INT,\n")
                .append("    ODS_UPDATE_DT_TM TIMESTAMPTZ\n")
                .append(");\n\n")
                .append("SET SEARCH_PATH TO ${schema};\n\n");

        logger.info("Generated Vertica DDL for table: {}", tableName);
        return ddl.toString();
    }


    /**
     * Generates Snowflake DDL for enhancing an existing table.
     *
     * @param metadata   List of column metadata.
     * @param tableName  The name of the table.
     * @return The generated Snowflake DDL statement.
     */
    public String generateSnowflakeEnhancementDDL(List<Map<String, Object>> metadata, List<String> newFields, String tableName) {
        StringBuilder ddl = new StringBuilder();

        ddl.append(String.format("migration_id=1,PHANALYTIC-(replace jira number):Adding %s columns to table %s\n", newFields, tableName));

        for (Map<String, Object> column : metadata) {
            String columnName = (String) column.get("COLUMN_NAME");
            String dataType = (String) column.get("DATA_TYPE");
            boolean nullable = (boolean) column.get("NULLABLE");
            String defaultValue = (String) column.get("DATA_DEFAULT");

            ddl.append("ALTER TABLE ").append("${schema}").append(".").append(tableName)
                    .append(" ADD COLUMN ").append(columnName).append(" ").append(dataType);

            if (!nullable) {
                ddl.append(" NOT NULL");
            }

            if (defaultValue != null) {
                ddl.append(" DEFAULT ").append(defaultValue);
            }

            ddl.append(";\n");
        }

        logger.info("Generated Snowflake enhancement DDL for table: {}", tableName);
        return ddl.toString();
    }

    /**
     * Generates Snowflake DDL for enhancing an existing table.
     *
     * @param metadata   List of column metadata.
     * @param tableName  The name of the table.
     * @return The generated Snowflake DDL statement.
     */
    public String generateVerticaEnhancementDDL(List<Map<String, Object>> metadata, List<String> newFields, String tableName) {
        StringBuilder ddl = new StringBuilder();

        ddl.append(String.format("migration_id=1,PHANALYTIC-(replace jira number):Adding %s columns to table %s\n", newFields, tableName));

        for (Map<String, Object> column : metadata) {
            String columnName = (String) column.get("COLUMN_NAME");
            String dataType = (String) column.get("DATA_TYPE");
            boolean nullable = (boolean) column.get("NULLABLE");
            String defaultValue = (String) column.get("DATA_DEFAULT");

            ddl.append("ALTER TABLE ").append("${schema}").append(".").append(tableName)
                    .append(" ADD COLUMN IF NOT EXISTS ").append(columnName).append(" ").append(dataType);

            if (!nullable) {
                ddl.append(" NOT NULL");
            }

            if (defaultValue != null) {
                ddl.append(" DEFAULT ").append(defaultValue);
            }

            ddl.append(";\n");
        }

        logger.info("Generated Vertica enhancement DDL for table: {}", tableName);
        return ddl.toString();
    }
}
