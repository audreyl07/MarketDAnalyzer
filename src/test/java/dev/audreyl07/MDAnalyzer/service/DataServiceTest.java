package dev.audreyl07.MDAnalyzer.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DataServiceTest {

    @Mock
    private QuestDBService questDBService;

    @InjectMocks
    private DataService dataService;

    private List<List<Object>> sampleOhlcvDataset; // matches column positions used by DataService
    private List<List<Object>> sampleAnalysisDataset; // [date, percentage]

    @BeforeEach
    void setUp() {
        // Two OHLCV rows
        sampleOhlcvDataset = List.of(
                List.of(
                        1, // unused id/col0
                        "2023-10-01T00:00:00.000000Z", // date at index 1
                        10.0, // open
                        12.0, // high
                        9.0, // low
                        11.0, // close
                        1000 // volume
                ),
                List.of(
                        1,
                        "2023-10-02T00:00:00.000000Z",
                        11.0,
                        13.0,
                        10.5,
                        12.5,
                        1500
                )
        );

        // Two analysis rows [date, percentage]
        sampleAnalysisDataset = List.of(
                List.of("2023-10-01T00:00:00.000000Z", 40.5),
                List.of("2023-10-02T00:00:00.000000Z", 42.0)
        );
    }

    // Helpers
    private Map<String, Object> execResponseWithDataset(List<List<Object>> dataset) {
        Map<String, Object> response = new HashMap<>();
        response.put("dataset", dataset);
        Map<String, Object> root = new HashMap<>();
        root.put("response", response);
        return root;
    }

    @Test
    void getData_stock_full_buildsQueryAndMapsFullRows() {
        // Given
        when(questDBService.executeQuery(anyString())).thenReturn(execResponseWithDataset(sampleOhlcvDataset));
        ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);

        // When
        List<Map<String, Object>> out = dataService.getData("stock", "full", "AAPL");

        // Then - query
        verify(questDBService).executeQuery(queryCaptor.capture());
        String q = queryCaptor.getValue();
        assertThat(q).contains("FROM historical_d");
        assertThat(q).contains("ticker = 'AAPL'");
        assertThat(q).contains("ORDER BY date ASC");

        // Then - mapping
        assertThat(out).hasSize(2);
        Map<String, Object> first = out.get(0);
        assertThat(first).containsEntry("time", 1696118400L); // 2023-10-01T00:00:00Z -> seconds
        assertThat(first).containsEntry("open", 10.0)
                         .containsEntry("high", 12.0)
                         .containsEntry("low", 9.0)
                         .containsEntry("close", 11.0)
                         .containsEntry("volume", 1000);
    }

    @Test
    void getData_stock_single_isDefaultAndMapsCompactRows() {
        // Given
        when(questDBService.executeQuery(anyString())).thenReturn(execResponseWithDataset(sampleOhlcvDataset));

        // When
        List<Map<String, Object>> out = dataService.getData("stock", "single", "MSFT");

        // Then - mapping
        assertThat(out).hasSize(2);
        Map<String, Object> first = out.get(0);
        assertThat(first).containsEntry("time", 1696118400L)
                         .containsEntry("value", 11.0)
                         .containsEntry("volume", 1000);
    }

    @Test
    void getData_index_full_queriesIndicesTable() {
        // Given
        when(questDBService.executeQuery(anyString())).thenReturn(execResponseWithDataset(sampleOhlcvDataset));
        ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);

        // When
        List<Map<String, Object>> out = dataService.getData("index", "full", "^GSPC");

        // Then
        verify(questDBService).executeQuery(queryCaptor.capture());
        String q = queryCaptor.getValue();
        assertThat(q).contains("FROM indices_d");
        assertThat(q).contains("ticker = '^GSPC'");
        assertThat(out).hasSize(2);
    }

    @Test
    void getData_unknownType_returnsEmpty_andDoesNotQuery() {
        // When
        List<Map<String, Object>> out = dataService.getData("crypto", "full", "BTC");

        // Then
        assertThat(out).isEmpty();
        verify(questDBService, never()).executeQuery(anyString());
    }

    @Test
    void getData_emptyDataset_returnsEmpty() {
        // Given
        when(questDBService.executeQuery(anyString())).thenReturn(execResponseWithDataset(Collections.emptyList()));

        // When
        List<Map<String, Object>> out = dataService.getData("stock", "full", "AAPL");

        // Then
        assertThat(out).isEmpty();
    }

    @Test
    void getAnalysis_high52w_buildsQueryAndMapsRows() {
        // Given
        when(questDBService.executeQuery(anyString())).thenReturn(execResponseWithDataset(sampleAnalysisDataset));
        ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);

        // When
        List<Map<String, Object>> out = dataService.getAnalysis("high52w");

        // Then
        verify(questDBService).executeQuery(queryCaptor.capture());
        String q = queryCaptor.getValue();
        assertThat(q).contains("FROM analysis_market");
        assertThat(q).contains("type = 'high52w'");
        assertThat(out).hasSize(2);
        assertThat(out.get(0)).containsEntry("time", 1696118400L)
                               .containsEntry("value", 40.5);
    }

    @Test
    void getAnalysis_low52w_buildsQueryAndMapsRows() {
        // Given
        when(questDBService.executeQuery(anyString())).thenReturn(execResponseWithDataset(sampleAnalysisDataset));

        // When
        List<Map<String, Object>> out = dataService.getAnalysis("low52w");

        // Then
        verify(questDBService).executeQuery(anyString());
        assertThat(out).hasSize(2);
        assertThat(out.get(1)).containsEntry("time", 1696204800L)
                               .containsEntry("value", 42.0);
    }

    @Test
    void getAnalysis_ma_50_200_usesUppercaseConstantInQuery_andMapsRows() {
        // Given
        when(questDBService.executeQuery(anyString())).thenReturn(execResponseWithDataset(sampleAnalysisDataset));
        ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);

        // When
        List<Map<String, Object>> out = dataService.getAnalysis("ma_50_200");

        // Then
        verify(questDBService).executeQuery(queryCaptor.capture());
        String q = queryCaptor.getValue();
        assertThat(q).contains("type = 'MA_50_200'");
        assertThat(out).hasSize(2);
        assertThat(out.get(0)).containsEntry("time", 1696118400L)
                               .containsEntry("value", 40.5);
    }

    @Test
    void getAnalysis_unknownType_returnsEmpty_andDoesNotQuery() {
        // When
        List<Map<String, Object>> out = dataService.getAnalysis("breadth");

        // Then
        assertThat(out).isEmpty();
        verify(questDBService, never()).executeQuery(anyString());
    }
}

