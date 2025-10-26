package dev.audreyl07.MDAnalyzer.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

class QuestDBServiceTest {

    private QuestDBService service;
    private QuestDBService spy;

    private Path tempDir;
    private Path errorDir;

    private com.sun.net.httpserver.HttpServer server;

    // Helper to start an HTTP server on a random port with a given context handler
    private int startServer(String path, com.sun.net.httpserver.HttpHandler handler) throws IOException {
        server = com.sun.net.httpserver.HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext(path, handler);
        server.start();
        return server.getAddress().getPort();
    }

    private static Map<String, String> parseQuery(String q) {
        Map<String, String> map = new LinkedHashMap<>();
        if (q == null || q.isEmpty()) return map;
        for (String part : q.split("&")) {
            int idx = part.indexOf('=');
            String k = idx >= 0 ? part.substring(0, idx) : part;
            String v = idx >= 0 ? part.substring(idx + 1) : "";
            map.put(URLDecoder.decode(k, StandardCharsets.UTF_8), URLDecoder.decode(v, StandardCharsets.UTF_8));
        }
        return map;
    }

    @BeforeEach
    void setUp() throws IOException {
        service = new QuestDBService();
        spy = Mockito.spy(service);
        // Set package-private fields directly (same package in tests)
        tempDir = Files.createTempDirectory("qdb_hist_");
        errorDir = Files.createTempDirectory("qdb_err_");
        spy.historicalDirectoryPath = tempDir.toString();
        spy.historicalErrorPath = errorDir.toString();
        spy.hostName = "localhost:9000"; // not used when we stub executeQuery
    }

    @AfterEach
    void tearDown() throws IOException {
        // Clean up created temp directories
        if (tempDir != null) {
            Files.walk(tempDir)
                    .sorted((a, b) -> b.compareTo(a))
                    .forEach(p -> {
                        try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                    });
        }
        if (errorDir != null) {
            Files.walk(errorDir)
                    .sorted((a, b) -> b.compareTo(a))
                    .forEach(p -> {
                        try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                    });
        }
        stopServerIfAny();
    }

    private void stopServerIfAny() {
    }

    private Map<String, Object> execResultWithResponse(Map<String, Object> response) {
        Map<String, Object> root = new HashMap<>();
        root.put("response", response);
        root.put("duration", 1);
        return root;
    }

    private Map<String, Object> execResultWithDataset(List<List<Object>> dataset) {
        Map<String, Object> response = new HashMap<>();
        response.put("dataset", dataset);
        return execResultWithResponse(response);
    }

    @Test
    void truncateTable_returnsTrue_whenDDL_OK_andBuildsCorrectQuery() {
        // Given
        Map<String, Object> response = new HashMap<>();
        response.put("ddl", "OK");
        doReturn(execResultWithResponse(response)).when(spy).executeQuery(anyString());
        ArgumentCaptor<String> qCap = ArgumentCaptor.forClass(String.class);

        // When
        boolean ok = spy.truncateTable("historical_d");

        // Then
        assertThat(ok).isTrue();
        verify(spy).executeQuery(qCap.capture());
        assertThat(qCap.getValue()).contains("TRUNCATE TABLE historical_d");
    }

    @Test
    void truncateTable_returnsFalse_whenDDL_notOK_orMissing() {
        // Case 1: ddl not OK
        Map<String, Object> response1 = new HashMap<>();
        response1.put("ddl", "FAIL");
        doReturn(execResultWithResponse(response1)).when(spy).executeQuery(anyString());
        assertThat(spy.truncateTable("t")).isFalse();

        // Case 2: ddl missing
        Map<String, Object> response2 = new HashMap<>();
        doReturn(execResultWithResponse(response2)).when(spy).executeQuery(anyString());
        assertThat(spy.truncateTable("t")).isFalse();
    }

    @Test
    void getLatestDate_buildsConditionOnlyWhenNonEmpty_andMapsDataset() {
        // Given: Answer to capture query and return a dataset
        doAnswer(invocation -> {
            String q = invocation.getArgument(0, String.class);
            // Assert WHERE clause presence based on provided condition
            if (q.contains("WHERE type = 'A'")) {
                return execResultWithDataset(List.of(List.of("20240101")));
            } else if (q.contains("WHERE")) {
                // unexpected other WHERE
                return execResultWithDataset(List.of());
            }
            return execResultWithDataset(List.of(List.of("20231231")));
        }).when(spy).executeQuery(anyString());

        // When: non-empty condition
        String withCond = spy.getLatestDate("analysis_market", "type = 'A'");
        // When: empty condition => no WHERE
        String noCond = spy.getLatestDate("historical_d", "");

        // Then
        assertThat(withCond).isEqualTo("20240101");
        assertThat(noCond).isEqualTo("20231231");
    }

    @Test
    void getLatestDate_returnsNull_whenDatasetNullOrEmpty_andEpochWhenFirstNull() {
        // Case 1: dataset null
        Map<String, Object> responseNull = new HashMap<>();
        responseNull.put("dataset", null);
        doReturn(execResultWithResponse(responseNull)).when(spy).executeQuery(anyString());
        assertThat(spy.getLatestDate("t", null)).isNull();

        // Case 2: dataset empty
        doReturn(execResultWithDataset(List.of())).when(spy).executeQuery(anyString());
        assertThat(spy.getLatestDate("t", null)).isNull();

        // Case 3: first value null -> epoch (use Arrays.asList; List.of disallows nulls)
        List<List<Object>> datasetWithNull = java.util.Arrays.asList(
                java.util.Arrays.asList((Object) null)
        );
        doReturn(execResultWithDataset(datasetWithNull)).when(spy).executeQuery(anyString());
        assertThat(spy.getLatestDate("t", null)).isEqualTo("19710101");
    }

    @Test
    void importFiles_onEmptyDirectory_returnsCountZero_andDurationPresent() throws IOException {
        // Create a hidden macOS metadata file that must be excluded
        Files.createFile(tempDir.resolve(".DS_Store"));

        Map<String, Object> result = spy.importFiles("historical_d");
        assertThat(result).containsKeys("duration", "count");
        assertThat((Number) result.get("count")).hasToString("0");
    }

    @Test
    void importFiles_onFailure_copiesFilesToErrorDirectory() throws IOException {
        // Arrange: create nested file under tempDir to verify mirror copy under errorDir
        Path subDir = Files.createDirectory(tempDir.resolve("sub"));
        Path srcFile = subDir.resolve("test.csv");
        Files.writeString(srcFile, "a,b,c\n1,2,3\n", StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        // Make HTTP fail fast: closed port
        spy.hostName = "127.0.0.1:1";

        // Act
        Map<String, Object> result = spy.importFiles("historical_d");

        // Assert: count remains 0 due to failures, and file is copied to errorDir mirror
        assertThat((Number) result.get("count")).hasToString("0");
        Path expectedError = Path.of(subDir.toString().replace(tempDir.toString(), errorDir.toString()))
                                 .resolve("test.csv");
        assertThat(Files.exists(expectedError)).isTrue();
        // Optional: content matches
        assertThat(Files.readString(expectedError)).contains("1,2,3");
    }

    @Test
    void executeQuery_success_parsesJson_andEchoesParams() throws Exception {
        // Arrange a minimal HTTP server that echoes received query params as JSON
        int port = startServer("/exec", exchange -> {
            Map<String, String> params = parseQuery(exchange.getRequestURI().getQuery());
            String json = String.format("{\"receivedQuery\":%s,\"receivedCount\":%s}",
                    toJsonString(params.getOrDefault("query", "")),
                    toJsonString(params.getOrDefault("count", "")));
            byte[] body = json.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(body); }
        });

        QuestDBService svc = new QuestDBService();
        svc.hostName = "127.0.0.1:" + port;

        // Act
        Map<String, Object> result = svc.executeQuery("SELECT 42");

        // Assert
        assertThat(result).containsKey("duration");
        Map<?, ?> response = (Map<?, ?>) result.get("response");
        assertThat(response).isNotNull();
        assertThat(response.get("receivedQuery")).isEqualTo("SELECT 42");
        assertThat(response.get("receivedCount")).isEqualTo("true");
    }

    @Test
    void executeQuery_noBody_returnsMapWithoutResponse() throws Exception {
        int port = startServer("/exec", exchange -> {
            exchange.sendResponseHeaders(204, -1); // No Content
            exchange.close();
        });
        QuestDBService svc = new QuestDBService();
        svc.hostName = "localhost:" + port;

        Map<String, Object> result = svc.executeQuery("SELECT 1");

        assertThat(result).containsKey("duration");
        assertThat(result).doesNotContainKey("response");
    }

    @Test
    void executeQuery_exception_doesNotThrow_andReturnsDurationOnly() {
        QuestDBService svc = new QuestDBService();
        svc.hostName = "127.0.0.1:1"; // closed port to force connect failure

        Map<String, Object> result = svc.executeQuery("SELECT 1");

        assertThat(result).containsKey("duration");
        assertThat(result).doesNotContainKey("response");
    }

    // Utility to JSON-escape a simple string
    private static String toJsonString(String s) {
        if (s == null) return "null";
        return '"' + s.replace("\\", "\\\\").replace("\"", "\\\"") + '"';
    }
}
