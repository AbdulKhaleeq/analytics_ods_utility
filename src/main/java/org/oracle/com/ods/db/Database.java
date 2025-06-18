package org.oracle.com.ods.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.oracle.com.ods.config.Config;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Database {
    private static final Logger logger = LoggerFactory.getLogger(Database.class);
    private static final String DB_URL = Config.getDbUrl();
    private static final String DB_USER = Config.getDbUser();
    private static final String DB_PASSWORD = Config.getDbPassword();

    private Database() {
    }

    /**
     * Creates connection to the millennium database.
     *
     * @return instance of the connection to database.
     */
    public static Connection getConnection() throws SQLException {
        logger.info("Connecting to the database...");
        try {
            return DriverManager.getConnection(
                    DB_URL, DB_USER, DB_PASSWORD
            );
        } catch (SQLException e) {
            logger.error("Error getting database connection", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Extracts database metadata for the connection.
     *
     * @return instance of the connection to database.
     */
    public static DatabaseMetaData getDatabaseMetaData() throws SQLException {
        Connection conn = getConnection();
        if (conn == null) {
            throw new IllegalArgumentException("Connection cannot be null.");
        }
        logger.info("Fetching database metadata...");
        DatabaseMetaData metaData = conn.getMetaData();
        logger.info("Successfully fetched database metadata.");
        return metaData;
    }
}