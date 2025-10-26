package dev.audreyl07.MDAnalyzer.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.audreyl07.MDAnalyzer.service.DataService;
import dev.audreyl07.MDAnalyzer.service.QuestDBService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.mockito.Mockito;

import java.util.*;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Unit tests for DataController.
 * Tests all endpoints and different data types (stock, index, market).
 */
@WebMvcTest(DataController.class)
@Import(DataControllerTest.TestConfig.class)
class DataControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private DataService dataService;

    @Test
    void getData_market_callsGetAnalysis_andReturnsJson() throws Exception {
        // Given
        List<Map<String, Object>> payload = List.of(
                Map.of("time", 1700000000L, "value", 12.3),
                Map.of("time", 1700086400L, "value", 15.7)
        );
        when(dataService.getAnalysis("high52w")).thenReturn(payload);

        // When & Then
        mockMvc.perform(get("/market/full/high52w")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(content().json(objectMapper.writeValueAsString(payload)));

        verify(dataService).getAnalysis("high52w");
    }

    @Test
    void getData_market_low52w_callsGetAnalysis() throws Exception {
        // Given
        List<Map<String, Object>> payload = List.of(
                Map.of("time", 1700000000L, "value", 8.5)
        );
        when(dataService.getAnalysis("low52w")).thenReturn(payload);

        // When & Then
        mockMvc.perform(get("/market/single/low52w")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().json(objectMapper.writeValueAsString(payload)));

        verify(dataService).getAnalysis("low52w");
    }

    @Test
    void getData_stock_full_callsGetData_andReturnsJson() throws Exception {
        // Given
        List<Map<String, Object>> payload = List.of(
                Map.of(
                        "time", 1700000100L,
                        "open", 150.0,
                        "high", 155.0,
                        "low", 149.0,
                        "close", 153.5,
                        "volume", 1000000
                )
        );
        when(dataService.getData("stock", "full", "AAPL")).thenReturn(payload);

        // When & Then
        mockMvc.perform(get("/stock/full/AAPL")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(content().json(objectMapper.writeValueAsString(payload)));

        verify(dataService).getData("stock", "full", "AAPL");
    }

    @Test
    void getData_stock_single_callsGetData_withCorrectParams() throws Exception {
        // Given
        List<Map<String, Object>> payload = List.of(
                Map.of("time", 1700000100L, "value", 153.5, "volume", 1000000)
        );
        when(dataService.getData("stock", "single", "MSFT")).thenReturn(payload);

        // When & Then
        mockMvc.perform(get("/stock/single/MSFT")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().json(objectMapper.writeValueAsString(payload)));

        verify(dataService).getData("stock", "single", "MSFT");
    }

    @Test
    void getData_index_full_callsGetData_withIndexType() throws Exception {
        // Given
        List<Map<String, Object>> payload = List.of(
                Map.of(
                        "time", 1700000200L,
                        "open", 4500.0,
                        "high", 4550.0,
                        "low", 4480.0,
                        "close", 4520.0,
                        "volume", 500000000
                )
        );
        when(dataService.getData("index", "full", "^GSPC")).thenReturn(payload);

        // When & Then
        mockMvc.perform(get("/index/full/^GSPC")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().json(objectMapper.writeValueAsString(payload)));

        verify(dataService).getData("index", "full", "^GSPC");
    }

    @Test
    void getData_index_single_callsGetData() throws Exception {
        // Given
        List<Map<String, Object>> payload = List.of(
                Map.of("time", 1700000200L, "value", 4520.0, "volume", 500000000)
        );
        when(dataService.getData("index", "single", "^DJI")).thenReturn(payload);

        // When & Then
        mockMvc.perform(get("/index/single/^DJI")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().json(objectMapper.writeValueAsString(payload)));

        verify(dataService).getData("index", "single", "^DJI");
    }

    @Test
    void getData_emptyResult_returnsEmptyArray() throws Exception {
        // Given
        List<Map<String, Object>> emptyPayload = Collections.emptyList();
        when(dataService.getData("stock", "full", "UNKNOWN")).thenReturn(emptyPayload);

        // When & Then
        mockMvc.perform(get("/stock/full/UNKNOWN")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().json("[]"));

        verify(dataService).getData("stock", "full", "UNKNOWN");
    }

    @Test
    void getData_market_ma_50_200_callsGetAnalysis() throws Exception {
        // Given
        List<Map<String, Object>> payload = List.of(
                Map.of("time", 1700000000L, "value", 45.2),
                Map.of("time", 1700086400L, "value", 47.8)
        );
        when(dataService.getAnalysis("ma_50_200")).thenReturn(payload);

        // When & Then
        mockMvc.perform(get("/market/full/ma_50_200")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().json(objectMapper.writeValueAsString(payload)));

        verify(dataService).getAnalysis("ma_50_200");
    }

    @Test
    void getData_multipleRecords_returnsCompleteList() throws Exception {
        // Given
        List<Map<String, Object>> payload = List.of(
                Map.of("time", 1700000100L, "value", 150.0, "volume", 1000000),
                Map.of("time", 1700086500L, "value", 151.5, "volume", 1100000),
                Map.of("time", 1700172900L, "value", 153.0, "volume", 1200000)
        );
        when(dataService.getData("stock", "single", "GOOGL")).thenReturn(payload);

        // When & Then
        mockMvc.perform(get("/stock/single/GOOGL")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(3))
                .andExpect(jsonPath("$[0].time").value(1700000100L))
                .andExpect(jsonPath("$[0].value").value(150.0))
                .andExpect(jsonPath("$[1].time").value(1700086500L))
                .andExpect(jsonPath("$[2].value").value(153.0));

        verify(dataService).getData("stock", "single", "GOOGL");
    }

    /**
     * Test configuration that provides mock beans for the test context.
     */
    static class TestConfig {
        @Bean
        public DataService dataService() {
            return Mockito.mock(DataService.class);
        }

        @Bean
        public QuestDBService questDBService() {
            return Mockito.mock(QuestDBService.class);
        }
    }
}

