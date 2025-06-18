package org.oracle.com.ods.services.schemaServices.vertica;

import org.oracle.com.ods.config.Config;
import org.oracle.com.ods.services.utility.HttpClientHelper;
import org.oracle.com.ods.services.utility.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Scanner;

/**
 * This class handles the deletion of a schema in Vertica by sending an HTTP DELETE request.
 * The schema name is provided by the user.
 */
public class DeleteVerticaSchema {
    private static final Logger logger = LoggerFactory.getLogger(DeleteVerticaSchema.class);

    /**
     * Main method that prompts the user for the schema name and initiates the schema deletion process.
     *
     * @param args Command line arguments (not used)
     */
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter the Vertica schema name to be deleted: ");
        String schemaName = scanner.nextLine().trim();

        if (schemaName.isEmpty()) {
            logger.error("Schema name cannot be empty. Exiting...");
            return;
        }

        try {
            String token = HttpClientHelper.getDevOAuthToken();
            String schemaId = HttpClientHelper.getSchemaId(Config.getVerticaSchemaUrl(), schemaName, token, false, SchemaType.VERTICA);

            if (schemaId == null || schemaId.isEmpty()) {
                logger.error("Failed to retrieve schema ID for schema: {}", schemaName);
                return;
            }

            logger.info("Schema ID retrieved: {}", schemaId);

            HttpClientHelper.deleteSchema(Config.getVerticaSchemaUrl(), schemaName, schemaId, token, false);
            logger.info("Schema Deleted Successfully: {}", schemaName);

        } catch (IOException e) {
            logger.error("An error occurred while processing the request for schema: {}", schemaName, e);
        } finally {
            scanner.close();
        }
    }
}
