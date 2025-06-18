package org.oracle.com.ods.services.mappingServices.snowflake;

import okhttp3.Response;
import org.oracle.com.ods.config.Config;
import org.oracle.com.ods.services.utility.HttpClientHelper;
import org.oracle.com.ods.services.utility.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Scanner;

/**
 * This class provides functionality to delete a specific version of a mapping in the staging environment.
 * The mapping is identified by its ID and version number.
 */
@Deprecated
public class DeleteStagingMappings {

    private static final Logger logger = LoggerFactory.getLogger(DeleteStagingMappings.class);

    /**
     * Main method that prompts the user for a mapping ID and version,
     * then attempts to delete the corresponding mapping from the staging environment.
     *
     * @param args Command line arguments (not used)
     * @throws IOException If there is an issue with the HTTP request
     */
    public static void main(String[] args) throws IOException {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter the mapping id: ");
        String mappingId = scanner.nextLine();

        if (mappingId == null || mappingId.trim().isEmpty()) {
            logger.error("Mapping ID cannot be empty. Exiting...");
            return;
        }

        System.out.print("Enter the version to be deleted: ");
        String targetVersion = scanner.nextLine();

        if (targetVersion == null || targetVersion.trim().isEmpty()) {
            logger.error("Version cannot be empty. Exiting...");
            return;
        }

        String token = HttpClientHelper.getStagingOAuthToken();

        String extractedId = HttpClientHelper.extractIdForMapping(mappingId, targetVersion, token, SchemaType.SNOWFLAKE);
        if (extractedId == null || extractedId.trim().isEmpty()) {
            logger.error("Couldn't find the record for the given version and mapping id. Exiting...");
            return;
        }
        System.out.println(extractedId);

        try (Response response = HttpClientHelper.delete(Config.getStagingModelMappingUrl(), extractedId, token, true)) {
            if (response.code() == 204) {
                logger.info("Mapping ID {} with version {} deleted successfully", mappingId, targetVersion);
            } else {
                String responseBody = response.body() != null ? response.body().string() : "No response body";
                throw new IOException("Failed to delete schema. Unexpected code " + response.code() + ": " + response.message() + " - " + responseBody);
            }
        }
    }
}
