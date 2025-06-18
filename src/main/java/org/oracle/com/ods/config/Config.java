package org.oracle.com.ods.config;

import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Config {
    private static final Logger logger = Logger.getLogger(Config.class.getName());
    private static final ResourceBundle bundle = ResourceBundle.getBundle("config");

    public static String getDbUrl() {
        try {
            return bundle.getString("db.url");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error retrieving db.url from config", e);
            throw e;
        }
    }

    public static String getDbUser() {
        try {
            return bundle.getString("db.user");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error retrieving db.user from config", e);
            throw e;
        }
    }

    public static String getDbPassword() {
        try {
            return bundle.getString("db.password");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error retrieving db.password from config", e);
            throw e;
        }
    }

    public static String getDbSchema() {
        try {
            return bundle.getString("db.schema");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error retrieving db.schema from config", e);
            throw e;
        }
    }

    public static String getVerticaSchemaUrl() {
        try {
            return bundle.getString("vertica.schema.url");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error retrieving vertica schema url from config", e);
            throw e;
        }
    }

    public static String getDevOAuthConsumerKey() {
        try {
            return bundle.getString("vertica.consumer.key");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error retrieving dev consumer key from config", e);
            throw e;
        }
    }

    public static String getDevOAuthConsumerSecret() {
        try {
            return bundle.getString("vertica.consumer.secret");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error retrieving dev consumer secret from config", e);
            throw e;
        }
    }

    public static String getDevOAuthAccessUrl() {
        try {
            return bundle.getString("vertica.oauth.url");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error retrieving dev oauth access url from config", e);
            throw e;
        }
    }

    public static String getSnowflakeSchemaUrl() {
        try {
            return bundle.getString("snowflake.schema.url");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error retrieving vertica schema url from config", e);
            throw e;
        }
    }

    public static String getStagingOAuthConsumerKey() {
        try {
            return bundle.getString("snowflake.consumer.key");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error retrieving dev consumer key from config", e);
            throw e;
        }
    }

    public static String getStagingOAuthConsumerSecret() {
        try {
            return bundle.getString("snowflake.consumer.secret");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error retrieving dev consumer secret from config", e);
            throw e;
        }
    }

    public static String getStagingOAuthAccessUrl() {
        try {
            return bundle.getString("snowflake.oauth.url");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error retrieving dev oauth access url from config", e);
            throw e;
        }
    }

    public static String getDevModelMappingUrl() {
        try {
            return bundle.getString("dev.model.mapping.url");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error retrieving dev model mapping url from config", e);
            throw e;
        }
    }

    public static String getStagingModelMappingUrl() {
        try {
            return bundle.getString("staging.model.mapping.url");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error retrieving staging model mapping url from config", e);
            throw e;
        }
    }

    public static String getProxyHost() {
        try {
            return bundle.getString("proxy.host");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error retrieving proxy host from config", e);
            throw e;
        }
    }

    public static int getProxyPortStaging() {
        try {
            return Integer.parseInt(bundle.getString("proxy.port.staging"));
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error retrieving proxy port staging from config", e);
            throw e;
        }
    }

    public static int getProxyPortSphereStage() {
        try {
            return Integer.parseInt(bundle.getString("proxy.port.spherestage"));
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error retrieving proxy port spherestage from config", e);
            throw e;
        }
    }

    public static String getModelMappingConfigPath() {
        try {
            return bundle.getString("model.mapping.config.path");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error retrieving the path for millennium model mapping configs", e);
            throw e;
        }
    }
}
