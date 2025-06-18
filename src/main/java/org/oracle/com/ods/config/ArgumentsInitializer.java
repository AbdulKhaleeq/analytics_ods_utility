package org.oracle.com.ods.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class ArgumentsInitializer {

    private static final Logger logger = LoggerFactory.getLogger(ArgumentsInitializer.class);

    private String tableName;
    private String avdlPath;
    private String entityType;
    private String outputDir;
    private List<String> newFields;
    private boolean isEnhancement;
    private String recordId;

    public ArgumentsInitializer(String[] args) {
        if (args.length == 4 && args[1].contains(",")) {
            this.isEnhancement = true;
            this.tableName = args[0];
            this.avdlPath = null;
            this.entityType = null;
            this.outputDir = args[3];
            this.newFields = Arrays.asList(args[1].split(","));
            this.recordId = args[2];

            logger.info("Arguments initialized: tableName={}, newFields={}, outputDir={}",
                    tableName, newFields, outputDir);
        } else if (args.length >= 4) {
            this.isEnhancement = false;
            this.tableName = args[0];
            this.avdlPath = args[1];
            this.entityType = args[2];
            this.outputDir = args[3];
            this.newFields = null;
            this.recordId = null;

            logger.info("Arguments initialized: tableName={}, avdlPath={}, entityType={}, outputDir={}",
                    tableName, avdlPath, entityType, outputDir);
        } else {
            throw new IllegalArgumentException("Invalid number of arguments provided.");
        }
    }

    public String getTableName() {
        return tableName;
    }

    public String getAvdlPath() {
        return avdlPath;
    }

    public String getEntityType() {
        return entityType;
    }

    public String getOutputDir() {
        return outputDir;
    }

    public List<String> getNewFields() {
        return newFields;
    }

    public boolean isEnhancement() {
        return isEnhancement;
    }

    public String getRecordId() {
        return recordId;
    }
}
