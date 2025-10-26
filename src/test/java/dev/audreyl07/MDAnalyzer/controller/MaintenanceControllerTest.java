package dev.audreyl07.MDAnalyzer.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.audreyl07.MDAnalyzer.service.MaintenanceService;
import dev.audreyl07.MDAnalyzer.service.QuestDBService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.atLeastOnce;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Unit tests for MaintenanceController.
 * Tests all maintenance endpoints for data pipeline operations.
 */
@WebMvcTest(MaintenanceController.class)
@Import(MaintenanceControllerTest.TestConfig.class)
class MaintenanceControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private MaintenanceService maintenanceService;

    @BeforeEach
    void resetMocks() {
        // Ensure no invocations from previous tests linger
        reset(maintenanceService);
    }

    @Test
    void importQuestDb_withValidType_returnsSuccess() throws Exception {
        // Given
        Map<String, Object> request = Map.of("type", "d");
        Map<String, Object> serviceResult = new HashMap<>();
        serviceResult.put("count", 100);
        serviceResult.put("duration", 5000L);
        when(maintenanceService.importRawFiles("d")).thenReturn(serviceResult);

        // When & Then
        mockMvc.perform(post("/maintenance/import-questdb")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.count").value(100))
                .andExpect(jsonPath("$.duration").value(5000));

        verify(maintenanceService, atLeastOnce()).importRawFiles("d");
    }

    @Test
    void importQuestDb_withEtfType_callsService() throws Exception {
        // Given
        Map<String, Object> request = Map.of("type", "etf_d");
        Map<String, Object> serviceResult = new HashMap<>();
        serviceResult.put("count", 50);
        when(maintenanceService.importRawFiles("etf_d")).thenReturn(serviceResult);

        // When & Then
        mockMvc.perform(post("/maintenance/import-questdb")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.count").value(50));

        verify(maintenanceService, atLeastOnce()).importRawFiles("etf_d");
    }

    @Test
    void importQuestDb_withIndicesType_callsService() throws Exception {
        // Given
        Map<String, Object> request = Map.of("type", "indices_d");
        Map<String, Object> serviceResult = new HashMap<>();
        serviceResult.put("count", 10);
        serviceResult.put("duration", 1000L);
        when(maintenanceService.importRawFiles("indices_d")).thenReturn(serviceResult);

        // When & Then
        mockMvc.perform(post("/maintenance/import-questdb")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true));

        verify(maintenanceService, atLeastOnce()).importRawFiles("indices_d");
    }

    @Test
    void importQuestDb_withEmptyRequest_usesDefaultType() throws Exception {
        // Given
        Map<String, Object> request = Map.of();
        Map<String, Object> serviceResult = new HashMap<>();
        serviceResult.put("count", 0);
        when(maintenanceService.importRawFiles("")).thenReturn(serviceResult);

        // When & Then
        mockMvc.perform(post("/maintenance/import-questdb")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk());

        verify(maintenanceService, atLeastOnce()).importRawFiles("");
    }

    @Test
    void insertIntoHistorical_withValidType_returnsSuccess() throws Exception {
        // Given
        Map<String, Object> request = Map.of("type", "d");
        Map<String, Object> serviceResult = new HashMap<>();
        serviceResult.put("rowsInserted", 500);
        when(maintenanceService.insertIntoHistorical("d")).thenReturn(serviceResult);

        // When & Then
        mockMvc.perform(post("/maintenance/insert-historical")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.rowsInserted").value(500));

        verify(maintenanceService).insertIntoHistorical("d");
    }

    @Test
    void insertIndicator52w_withValidType_returnsSuccess() throws Exception {
        // Given
        Map<String, Object> request = Map.of("type", "d");
        Map<String, Object> serviceResult = new HashMap<>();
        serviceResult.put("processed", 200);
        when(maintenanceService.insertIntoIndicator52w("d")).thenReturn(serviceResult);

        // When & Then
        mockMvc.perform(post("/maintenance/insert-52w")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.processed").value(200));

        verify(maintenanceService).insertIntoIndicator52w("d");
    }

    @Test
    void insertAnalysis52w_withValidType_returnsSuccess() throws Exception {
        // Given
        Map<String, Object> request = Map.of("type", "high52w");
        Map<String, Object> serviceResult = new HashMap<>();
        serviceResult.put("analysisRecords", 30);
        when(maintenanceService.insertIntoAnalysis52w("high52w")).thenReturn(serviceResult);

        // When & Then
        mockMvc.perform(post("/maintenance/insert-analysis52w")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.analysisRecords").value(30));

        verify(maintenanceService).insertIntoAnalysis52w("high52w");
    }

    @Test
    void update52w_withValidType_returnsSuccess() throws Exception {
        // Given
        Map<String, Object> request = Map.of("type", "low52w");
        Map<String, Object> serviceResult = new HashMap<>();
        serviceResult.put("updated", 25);
        when(maintenanceService.updateAnalysis52w("low52w")).thenReturn(serviceResult);

        // When & Then
        mockMvc.perform(post("/maintenance/update-52w")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.updated").value(25));

        verify(maintenanceService).updateAnalysis52w("low52w");
    }

    @Test
    void insertIndicatorMA_withAllParams_returnsSuccess() throws Exception {
        // Given
        Map<String, Object> request = Map.of(
                "type", "d",
                "interval", 50,
                "truncate", true
        );
        Map<String, Object> serviceResult = new HashMap<>();
        serviceResult.put("maRecords", 1000);
        when(maintenanceService.insertIntoIndicatorMA("d", 50, true)).thenReturn(serviceResult);

        // When & Then
        mockMvc.perform(post("/maintenance/insert-MA")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.maRecords").value(1000));

        verify(maintenanceService).insertIntoIndicatorMA("d", 50, true);
    }

    @Test
    void insertIndicatorMA_withDefaultValues_usesDefaults() throws Exception {
        // Given
        Map<String, Object> request = Map.of("type", "etf_d");
        Map<String, Object> serviceResult = new HashMap<>();
        serviceResult.put("maRecords", 500);
        when(maintenanceService.insertIntoIndicatorMA("etf_d", 0, false)).thenReturn(serviceResult);

        // When & Then
        mockMvc.perform(post("/maintenance/insert-MA")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true));

        verify(maintenanceService).insertIntoIndicatorMA("etf_d", 0, false);
    }

    @Test
    void insertIndicatorMACompare_withIntervals_returnsSuccess() throws Exception {
        // Given
        Map<String, Object> request = Map.of(
                "type", "d",
                "interval", 50,
                "second_interval", 200
        );
        Map<String, Object> serviceResult = new HashMap<>();
        serviceResult.put("compareRecords", 800);
        when(maintenanceService.insertIntoIndicatorMACompare("d", 50, 200)).thenReturn(serviceResult);

        // When & Then
        mockMvc.perform(post("/maintenance/insert-MACompare")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.compareRecords").value(800));

        verify(maintenanceService).insertIntoIndicatorMACompare("d", 50, 200);
    }

    @Test
    void insertAnalysisMA_withValidType_returnsSuccess() throws Exception {
        // Given
        Map<String, Object> request = Map.of("type", "MA_50_200");
        Map<String, Object> serviceResult = new HashMap<>();
        serviceResult.put("analysisRows", 100);
        when(maintenanceService.insertIntoAnalysisMA("MA_50_200")).thenReturn(serviceResult);

        // When & Then
        mockMvc.perform(post("/maintenance/insert-analysisMA")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.analysisRows").value(100));

        verify(maintenanceService).insertIntoAnalysisMA("MA_50_200");
    }

    @Test
    void updateMA_withBothIntervals_returnsSuccess() throws Exception {
        // Given
        Map<String, Object> request = Map.of(
                "type", "d",
                "first_interval", 50,
                "second_interval", 200
        );
        Map<String, Object> serviceResult = new HashMap<>();
        serviceResult.put("updatedRows", 50);
        when(maintenanceService.updateAnalysisMA("d", 50, 200)).thenReturn(serviceResult);

        // When & Then
        mockMvc.perform(post("/maintenance/update-MA")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.updatedRows").value(50));

        verify(maintenanceService).updateAnalysisMA("d", 50, 200);
    }

    @Test
    void getLatest_withTableAndType_returnsLatestDate() throws Exception {
        // Given
        Map<String, Object> request = Map.of(
                "table", "historical_d",
                "type", "d"
        );
        when(maintenanceService.getLatestDate("historical_d", "d")).thenReturn("20251026");

        // When & Then
        mockMvc.perform(post("/maintenance/latest")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.latest").value("20251026"));

        verify(maintenanceService).getLatestDate("historical_d", "d");
    }

    @Test
    void getLatest_withTableOnly_returnsLatestDate() throws Exception {
        // Given
        Map<String, Object> request = Map.of("table", "indices_d");
        when(maintenanceService.getLatestDate("indices_d", "")).thenReturn("20251025");

        // When & Then
        mockMvc.perform(post("/maintenance/latest")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.latest").value("20251025"));

        verify(maintenanceService).getLatestDate("indices_d", "");
    }

    @Test
    void getLatest_withEmptyRequest_usesDefaults() throws Exception {
        // Given
        Map<String, Object> request = Map.of();
        when(maintenanceService.getLatestDate("", "")).thenReturn("19710101");

        // When & Then
        mockMvc.perform(post("/maintenance/latest")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.latest").value("19710101"));

        verify(maintenanceService).getLatestDate("", "");
    }

    @Test
    void importQuestDb_withNullResult_stillReturnsSuccess() throws Exception {
        // Given
        Map<String, Object> request = Map.of("type", "d");
        Map<String, Object> serviceResult = new HashMap<>();
        when(maintenanceService.importRawFiles("d")).thenReturn(serviceResult);

        // When & Then
        mockMvc.perform(post("/maintenance/import-questdb")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true));

        verify(maintenanceService, atLeastOnce()).importRawFiles("d");
    }

    /**
     * Test configuration that provides mock beans for the test context.
     */
    static class TestConfig {
        @Bean
        public MaintenanceService maintenanceService() {
            return Mockito.mock(MaintenanceService.class);
        }

        @Bean
        public QuestDBService questDBService() {
            return Mockito.mock(QuestDBService.class);
        }
    }
}
