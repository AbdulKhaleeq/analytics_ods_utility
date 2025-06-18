package org.oracle.com.ods.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.io.IOException;
import java.io.FileWriter;

public class FileWriterUtil {

    /**
     * Writes the given JSON content to a file.
     *
     * @param modelMapping the JSON content to write
     * @param filePath the path to the file where JSON content should be written
     * @param mappingId to create file name same as mappingId
     */
    public static void writeJsonToFile(ObjectNode modelMapping, String filePath, String mappingId) {
        writeFile(modelMapping, filePath, mappingId);
    }

    private static void writeFile(ObjectNode modelMapping, String filePath, String mappingId) {
        String outputPath = filePath + mappingId + ".json";
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.configure(com.fasterxml.jackson.core.JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);

        try {
            mapper.writeValue(new File(outputPath), modelMapping);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error writing to file: " + outputPath, e);
        }
    }

    /**
     * Writes the given text content to a file.
     *
     * @param textContent the text content to write
     * @param filePath the path to the file where text content should be written
     * @param mappingId to create file name same as mappingId
     */
    public static void writeTextToFile(String textContent, String filePath, String mappingId) {
        writeMigrationFile(textContent, filePath, mappingId);
    }

    private static void writeMigrationFile(String textContent, String filePath, String mappingId) {
        String outputPath = filePath + mappingId + ".migration";
        try {
            File file = new File(outputPath);
            file.getParentFile().mkdirs();

            try (FileWriter writer = new FileWriter(file)) {
                writer.write(textContent);
                writer.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error writing to file: " + filePath, e);
        }
    }

    /**
     * Writes the given compressed json content to a file.
     *
     * @param compressedJson the text content to write
     * @param filePath the path to the file where text content should be written
     */
    public static void writeCompressedJsonToFile(String compressedJson, String filePath) {
        writeCompressedJson(compressedJson, filePath);
    }

    private static void writeCompressedJson(String compressedJson, String filePath) {
        String outputPath = filePath + "compressed_json.txt";
        try {
            File file = new File(outputPath);
            file.getParentFile().mkdirs();

            try (FileWriter writer = new FileWriter(file)) {
                writer.write(compressedJson);
                writer.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error writing to file: " + filePath, e);
        }
    }
}
