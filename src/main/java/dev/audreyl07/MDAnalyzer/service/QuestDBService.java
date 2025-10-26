package dev.audreyl07.MDAnalyzer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

/**
 * Service for interacting with QuestDB via HTTP endpoints.
 *
 * Features:
 * - Execute SQL queries and return parsed JSON responses
 * - Import CSV files into QuestDB tables (multipart uploads)
 * - Copy failed imports to an error directory for triage
 * - Utility operations: truncate table, get latest processed date
 */
@Service
public class QuestDBService {

    String importUrlTemplate = "http://%s/imp?fmt=json&forceHeader=true&name=%s";

    String execUrlTemplate = "http://%s/exec";

    @Value("${mdanalyzer.path.historicalDirectoryPath}")
    String historicalDirectoryPath;

    @Value("${mdanalyzer.path.historicalErrorPath}")
    String historicalErrorPath;

    @Value("${mdanalyzer.hostName}")
    String hostName;

    public Boolean truncateTable(String table) {
        String query = String.format("TRUNCATE TABLE %s", table);
        Map<String, Object> result = executeQuery(query);
        Map<String, Object> response = (Map<String, Object>) result.get("response");
        String ddl = (String) response.get("ddl");
        return "OK".equals(ddl);
    }

    public String getLatestDate(String table, String condition) {
        String query = String.format("SELECT CAST(TO_STR(MAX(date), 'yyyyMMdd') AS INT) AS MAX FROM %s", table);
        if (condition != null && !condition.isEmpty()) {
            query += " WHERE " + condition;
        }
        Map<String, Object> result = executeQuery(query);
        Map<String, Object> response = (Map<String, Object>) result.get("response");
        List<Object> dataset = (List<Object>) response.get("dataset");
        if (dataset == null || dataset.isEmpty()) {
            return null;
        }
        List<Object> firstRecord = (List<Object>) dataset.get(0);
        return firstRecord.get(0) == null ? "19710101" : firstRecord.get(0).toString();
    }

    public Map<String, Object> importFiles(String table) {
        Path startPath = Paths.get(historicalDirectoryPath);
        String url = String.format(importUrlTemplate, hostName, table);
        long start = System.currentTimeMillis();
        Map<String, Object> map = new HashMap<>();
        int count = 0;
        try {
            List<String> fileNames = listFiles(startPath);
            for (String fullFileName : fileNames) {
                if (importFile(url, fullFileName, startPath.toAbsolutePath().toString(), historicalErrorPath)) {
                    count++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println("duration: " + (end - start));
        map.put("duration", end - start);
        map.put("count", count);
        return map;
    }

    private List<String> listFiles(Path startPath) throws IOException {
        List<String> fileNames = new ArrayList<>();
        Files.walkFileTree(startPath, EnumSet.noneOf(FileVisitOption.class), Integer.MAX_VALUE, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (!file.getFileName().startsWith(".DS_Store")) {
                    fileNames.add(file.toString());
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
                System.err.println("Failed to visit file: " + file.toString() + " (" + exc.getMessage() + ")");
                return FileVisitResult.CONTINUE;
            }
        });
        return fileNames;
    }

    private boolean importFile(String url, String fileName, String importHistoricalFilePath, String errorPath) {
        System.out.println(fileName);
        File file = new File(fileName);
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost uploadFile = new HttpPost(url);

            MultipartEntityBuilder builder = MultipartEntityBuilder.create();
            builder.addBinaryBody("data", file);

            HttpEntity multipart = builder.build();
            uploadFile.setEntity(multipart);

            HttpResponse response = httpClient.execute(uploadFile);
            HttpEntity responseEntity = response.getEntity();

            if (responseEntity != null) {
                String responseString = EntityUtils.toString(responseEntity);
                System.out.println("Response: " + responseString);
            }
            return true;
        } catch (Exception e) {
            System.out.println("ERROR:" + fileName);
            copyToErrorDirectory(file, importHistoricalFilePath, errorPath);
        }
        return false;
    }

    private void copyToErrorDirectory(File file, String importHistoricalFilePath, String errorPath) {
        try {
            String parentPath = file.getParent();
            String writePath = parentPath.replace(importHistoricalFilePath, errorPath);
            Path writeDir = Paths.get(writePath);
            if (!Files.exists(writeDir)) {
                Files.createDirectories(writeDir);
            }
            Path targetPath = writeDir.resolve(file.getName());
            Files.copy(file.toPath(), targetPath);
            System.out.println("File copied to /error/ directory: " + targetPath);
        } catch (IOException ioException) {
            System.err.println("Failed to copy file to /error/ directory: " + ioException.getMessage());
            ioException.printStackTrace();
        }
    }

    public Map<String, Object> executeQuery(String query) {
        String url = String.format(execUrlTemplate, hostName);
        String count = "true";
        Map<String, Object> map = new HashMap<>();
        long start = System.currentTimeMillis();
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            System.out.println("Query:\n" + query);
            URI uri = new URIBuilder(url)
                    .addParameter("query", query)
                    .addParameter("count", count)
                    .build();

            HttpGet request = new HttpGet(uri);

            HttpResponse response = httpClient.execute(request);
            HttpEntity entity = response.getEntity();

            if (entity != null) {
                String responseString = EntityUtils.toString(entity);
                ObjectMapper mapper = new ObjectMapper();
                Map<String, Object> responseMap = mapper.readValue(responseString, Map.class);
                map.put("response", responseMap);
//                System.out.println("Response: " + responseString);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println("duration: " + (end - start));
        map.put("duration", end - start);
        return map;
    }
}
