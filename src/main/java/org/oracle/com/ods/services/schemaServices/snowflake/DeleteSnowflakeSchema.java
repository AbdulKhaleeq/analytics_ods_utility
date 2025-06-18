package org.oracle.com.ods.services.schemaServices.snowflake;

import org.oracle.com.ods.config.Config;
import org.oracle.com.ods.services.utility.HttpClientHelper;
import org.oracle.com.ods.services.utility.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Scanner;

/**
 * This class handles the deletion of an existing Snowflake schema by interacting with
 * a remote service via an HTTP DELETE request. The schema name is provided by the user.
 */
public class DeleteSnowflakeSchema {

    private static final Logger logger = LoggerFactory.getLogger(DeleteSnowflakeSchema.class);

    /**
     * Main method that prompts the user for the schema name and initiates the schema deletion process.
     *
     * @param args Command line arguments (not used)
     */
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter the Snowflake schema name to be deleted: ");
        String schemaName = scanner.nextLine().trim();

        if (schemaName.isEmpty()) {
            System.out.println("Schema name cannot be empty. Exiting...");
            return;
        }

        deleteSnowflakeSchema(schemaName);
    }

    /**
     * Method to delete an existing Snowflake schema by sending an HTTP DELETE request with the schema details.
     *
     * @param schemaName The name of the schema to be deleted
     */
    private static void deleteSnowflakeSchema(String schemaName) {
        try {
            String token = HttpClientHelper.getStagingOAuthToken();

            String schemaId = HttpClientHelper.getSchemaId(
                    Config.getSnowflakeSchemaUrl(),
                    schemaName,
                    token,
                    true,
                    SchemaType.SNOWFLAKE
            );

            logger.info("Schema ID retrieved: {}", schemaId);

            HttpClientHelper.deleteSchema(
                    Config.getSnowflakeSchemaUrl(),
                    schemaName,
                    schemaId,
                    token,
                    true
            );

            logger.info("Schema deleted successfully: {}", schemaName);
        } catch (IOException e) {
            logger.error("An error occurred while processing the request to delete schema: {}", schemaName, e);
        }
    }
}
