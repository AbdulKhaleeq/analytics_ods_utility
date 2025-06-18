package org.oracle.com.ods.db;

import java.sql.*;
import java.util.*;

import org.oracle.com.ods.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrimaryKeyExtractor {

    private static final Logger logger = LoggerFactory.getLogger(PrimaryKeyExtractor.class);

    private PrimaryKeyExtractor() {
    }

    /**
     * Extracts Set of primary keys for the input table from db
     *
     * @param tableName  Input table name.
     * @return primaryKeys which contains all the primary keys for the given table.
     */
    public static Set<String> extractPrimaryKeys(String tableName) {
        Set<String> primaryKeys = new HashSet<>();
        try {
             DatabaseMetaData metaData = Database.getDatabaseMetaData();
            try (ResultSet pkResultSet = metaData.getPrimaryKeys(null, Config.getDbSchema(), tableName.toUpperCase())) {
                while (pkResultSet.next()) {
                    primaryKeys.add(pkResultSet.getString("COLUMN_NAME"));
                }
            }

            // If no primary keys found, fetch unique indexes
            if (primaryKeys.isEmpty()) {
                try (ResultSet indexResultSet = metaData.getIndexInfo(null, Config.getDbSchema(), tableName.toUpperCase(), true, true)) {
                    while (indexResultSet.next()) {
                        String indexName = indexResultSet.getString("INDEX_NAME");
                        boolean nonUnique = indexResultSet.getBoolean("NON_UNIQUE");
                        if (indexName != null && !nonUnique) {
                            primaryKeys.add(indexResultSet.getString("COLUMN_NAME"));
                        }
                    }
                }
            }

        } catch (SQLException e) {
            logger.error("Error extracting primary keys for table: " + tableName, e);
        }
        return primaryKeys;
    }
}
