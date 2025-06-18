package org.oracle.com.ods.services.schemaServices.vertica;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.oracle.com.ods.config.Config;
import org.oracle.com.ods.services.utility.HttpClientHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Scanner;

/**
 * This class handles the creation of a new LD (Logical Domain) schema in Vertica by interacting with
 * a remote service via an HTTP POST request. The schema name is provided by the user.
 */
public class CreateVerticaLdSchema {

    private static final Logger logger = LoggerFactory.getLogger(CreateVerticaLdSchema.class);

    /**
     * Main method that prompts the user for the schema name and initiates the schema creation process.
     *
     * @param args Command line arguments (not used)
     * @throws IOException If an I/O error occurs
     */
    public static void main(String[] args) throws IOException {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter the new LD schema name: ");
        String schemaName = scanner.nextLine().trim();

        if (schemaName.isEmpty()) {
            logger.error("Schema name cannot be empty. Exiting...");
            return;
        }

        createVerticaLdSchema(schemaName);
    }

    /**
     * Method to create a new LD schema in Vertica by sending an HTTP POST request with the schema details.
     *
     * @param schemaName The name of the schema to be created
     * @throws IOException If an I/O error occurs during the HTTP request
     */
    private static void createVerticaLdSchema(String schemaName) throws IOException {
        String token = HttpClientHelper.getDevOAuthToken();

        ObjectMapper objectMapper = new ObjectMapper();

        ObjectNode logicalDomainSchemaBody = objectMapper.createObjectNode()
                .put("name", schemaName)
                .put("schema_type", "MILLENNIUM")
                .put("cluster_id", 44)
                .put("client_id", "e92ca89f-8524-451a-9346-666d6ff39329")
                .put("domain", "string")
                .put("active", false)
                .put("custom_edw_suffix", "string")
                .putNull("custom_edw_role")
                .put("cluster_load_balancer_address", "10.190.160.30")
                .put("cluster_vip", "10.190.160.30");

        ObjectNode attributesNode = objectMapper.createObjectNode();
        attributesNode.put("source", "e2de4a37-dfa5-4e92-83ee-ee1610806a06#20210101");

        logicalDomainSchemaBody.set("attributes", attributesNode);

        try {
            HttpClientHelper.post(Config.getVerticaSchemaUrl(), logicalDomainSchemaBody, token, false);
            logger.info("Schema Created Successfully: {}", schemaName);
        } catch (IOException e) {
            logger.error("Failed to create schema: {}", schemaName, e);
        }
    }
}
