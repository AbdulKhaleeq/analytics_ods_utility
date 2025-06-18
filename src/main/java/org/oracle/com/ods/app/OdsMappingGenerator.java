package org.oracle.com.ods.app;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.oracle.com.ods.config.ArgumentsInitializer;
import org.oracle.com.ods.db.MetadataExtractor;
import org.oracle.com.ods.db.PrimaryKeyExtractor;
import org.oracle.com.ods.models.ModelMappingGenerator;
import org.oracle.com.ods.models.RecordMapGenerator;
import org.oracle.com.ods.models.TargetModelGenerator;
import org.oracle.com.ods.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class OdsMappingGenerator {

    private static final Logger logger = LoggerFactory.getLogger(OdsMappingGenerator.class);

    /**
     * Generates model mapping file and migration files for snowflake and vertica databse.
     * Requires five mandatory arguments to be provided to generate the files.
     *
     * Order of arguments:
     * Table name, avdl file path, entity type for the table, output directory path, raw schema from schema_store.yaml
     *
     * Writes the files to the given output directory.
     */
    public static void main(String[] args) {
        try {
            ArgumentsInitializer arguments = new ArgumentsInitializer(args);

            if (arguments.isEnhancement()) {
                handleEnhancement(arguments);
            } else {
                handleNewTable(arguments);
            }
        } catch (Exception e) {
            logger.error("An error occurred", e);
            System.exit(1);
        }
    }

    private static void handleNewTable(ArgumentsInitializer arguments) {
        try {
            logger.info("Reading avdl file from path: {}", arguments.getAvdlPath());
            AvdlParser avdlParser = new AvdlParser(arguments.getAvdlPath());
            List<String> avdlFields = avdlParser.getFields();
            String recordId = avdlParser.getRecordId();

            logger.info("Extracting metadata for table: {}", arguments.getTableName());
            List<Map<String, Object>> metadata = MetadataExtractor.extractMetadata(arguments.getTableName(), avdlFields, null);

            ObjectNode recordMap = RecordMapGenerator.generateRecordMap(metadata, arguments.getTableName(), recordId);
            logger.info("Generated record map successfully");

            Set<String> primaryKeys = PrimaryKeyExtractor.extractPrimaryKeys(arguments.getTableName());
            ArrayNode targetModels = TargetModelGenerator.generateTargetModels(metadata, arguments.getTableName(), primaryKeys, arguments.isEnhancement());
            logger.info("Generated target models successfully");

            String mappingId = UUID.randomUUID().toString();
            String schema = OdsSchema.getEncodedString(avdlParser.getJsonSchema());

            ObjectNode modelMapping = ModelMappingGenerator.generateModelMapping(recordMap, targetModels, mappingId, arguments.getEntityType(), schema);
            FileWriterUtil.writeJsonToFile(modelMapping, arguments.getOutputDir(), mappingId);
            logger.info("Generated model mapping successfully");

            MigrationGenerator migrations = new MigrationGenerator();

            String adwDdl = migrations.generateAdwDDL(metadata, arguments.getTableName());
            String adwMappingId = mappingId + "-adw";
            FileWriterUtil.writeTextToFile(adwDdl, arguments.getOutputDir(), adwMappingId);

            String snowflakeDDL = migrations.generateSnowflakeDDL(metadata, arguments.getTableName());
            String snowflakeMappingId = mappingId + "-snowflake";
            FileWriterUtil.writeTextToFile(snowflakeDDL, arguments.getOutputDir(), snowflakeMappingId);

            String verticaDDL = migrations.generateVerticaDDL(metadata, arguments.getTableName(), primaryKeys);
            FileWriterUtil.writeTextToFile(verticaDDL, arguments.getOutputDir(), mappingId);


            String compressedJson = JsonCompactor.compactAndEscapeJson(modelMapping);
            FileWriterUtil.writeCompressedJsonToFile(compressedJson, arguments.getOutputDir());
            logger.info("Compressed model mapping json successfully");
        } catch (Exception e) {
            logger.error("An error occurred", e);
        }
    }

    private static void handleEnhancement(ArgumentsInitializer arguments) {
        try {
            logger.info("Extracting metadata for new fields: {}", arguments.getNewFields());
            List<Map<String, Object>> metadata = MetadataExtractor.extractMetadata(arguments.getTableName(), null, arguments.getNewFields());

            String recordId = arguments.getRecordId();
            Set<String> primaryKeys = null;
            String mappingId = arguments.getTableName();

            ObjectNode recordMap = RecordMapGenerator.generateRecordMap(metadata, arguments.getTableName(), recordId);
            logger.info("Generated record map successfully");

            ArrayNode targetModels = TargetModelGenerator.generateTargetModels(metadata, arguments.getTableName(), primaryKeys, arguments.isEnhancement());
            logger.info("Generated target models successfully");

            ObjectNode modelMapping = ModelMappingGenerator.generateModelMappingForEnhancement(recordMap, targetModels);
            FileWriterUtil.writeJsonToFile(modelMapping, arguments.getOutputDir(), mappingId);
            logger.info("Generated model mapping successfully");

            MigrationGenerator migrations = new MigrationGenerator();
            String snowflakeDDL = migrations.generateSnowflakeEnhancementDDL(metadata, arguments.getNewFields(), arguments.getTableName());
            String mapping_id = mappingId + "-snowflake";
            FileWriterUtil.writeTextToFile(snowflakeDDL, arguments.getOutputDir(), mapping_id);

            String verticaDDL = migrations.generateVerticaEnhancementDDL(metadata, arguments.getNewFields(), arguments.getTableName());
            FileWriterUtil.writeTextToFile(verticaDDL, arguments.getOutputDir(), mappingId);

        } catch (Exception e) {
            logger.error("An error occurred while handling enhancement", e);
        }
    }
}
