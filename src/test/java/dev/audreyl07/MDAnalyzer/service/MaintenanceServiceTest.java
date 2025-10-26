package dev.audreyl07.MDAnalyzer.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MaintenanceServiceTest {

    @Mock
    private QuestDBService questDBService;

    @Spy
    @InjectMocks
    private MaintenanceService maintenanceService;

    private Map<String, Object> falseMap;

    @BeforeEach
    void init() {
        falseMap = new HashMap<>();
        falseMap.put("success", Boolean.FALSE);
    }

    // Helpers
    private Map<String, Object> dmlOkResult(int duration) {
        Map<String, Object> response = new HashMap<>();
        response.put("dml", "OK");
        Map<String, Object> root = new HashMap<>();
        root.put("response", response);
        root.put("duration", duration);
        return root;
    }

    private Map<String, Object> dmlFailResult(int duration) {
        Map<String, Object> response = new HashMap<>();
        response.put("dml", "FAIL");
        Map<String, Object> root = new HashMap<>();
        root.put("response", response);
        root.put("duration", duration);
        return root;
    }

    // getLatestDate
    @Test
    void getLatestDate_forAnalysisTables_buildsTypeConditionWhenTypeProvided() {
        when(questDBService.getLatestDate(eq("analysis_market"), anyString())).thenReturn("20250101");
        String result = maintenanceService.getLatestDate("analysis_market", "high52w");
        ArgumentCaptor<String> condCap = ArgumentCaptor.forClass(String.class);
        verify(questDBService).getLatestDate(eq("analysis_market"), condCap.capture());
        assertThat(condCap.getValue()).isEqualTo("type = 'high52w'");
        assertThat(result).isEqualTo("20250101");
    }

    @Test
    void getLatestDate_forAnalysisTables_emptyTypePassesEmptyCondition() {
        when(questDBService.getLatestDate(eq("indicator_d_52w"), anyString())).thenReturn("20240101");
        String result = maintenanceService.getLatestDate("indicator_d_52w", "");
        ArgumentCaptor<String> condCap = ArgumentCaptor.forClass(String.class);
        verify(questDBService).getLatestDate(eq("indicator_d_52w"), condCap.capture());
        assertThat(condCap.getValue()).isEqualTo("");
        assertThat(result).isEqualTo("20240101");
    }

    @Test
    void getLatestDate_forOtherTables_passesNullCondition() {
        when(questDBService.getLatestDate(eq("historical_d"), isNull())).thenReturn("20230101");
        String result = maintenanceService.getLatestDate("historical_d", "ignored");
        verify(questDBService).getLatestDate("historical_d", null);
        assertThat(result).isEqualTo("20230101");
    }

    // importRawFiles
    @Test
    void importRawFiles_invalidType_returnsFalseMap_andNoCalls() {
        Map<String, Object> out = maintenanceService.importRawFiles("abc");
        assertThat(out).isEqualTo(falseMap);
        verify(questDBService, never()).truncateTable(anyString());
        verify(questDBService, never()).importFiles(anyString());
    }

    @Test
    void importRawFiles_truncateFailure_returnsFalseMap() {
        when(questDBService.truncateTable("historical_raw_d")).thenReturn(false);
        Map<String, Object> out = maintenanceService.importRawFiles("d");
        assertThat(out).isEqualTo(falseMap);
        verify(questDBService).truncateTable("historical_raw_d");
        verify(questDBService, never()).importFiles(anyString());
    }

    @Test
    void importRawFiles_d_callsTruncateThenImport_andReturnsResult() {
        when(questDBService.truncateTable("historical_raw_d")).thenReturn(true);
        Map<String, Object> importRes = new HashMap<>();
        importRes.put("count", 10);
        when(questDBService.importFiles("historical_raw_d")).thenReturn(importRes);
        Map<String, Object> out = maintenanceService.importRawFiles("d");
        assertThat(out).isEqualTo(importRes);
        verify(questDBService).truncateTable("historical_raw_d");
        verify(questDBService).importFiles("historical_raw_d");
    }

    @Test
    void importRawFiles_etf_and_indices_paths() {
        when(questDBService.truncateTable(anyString())).thenReturn(true);
        when(questDBService.importFiles(anyString())).thenReturn(Map.of("ok", true));
        maintenanceService.importRawFiles("etf_d");
        verify(questDBService).truncateTable("historical_raw_etf_d");
        verify(questDBService).importFiles("historical_raw_etf_d");
        maintenanceService.importRawFiles("indices_d");
        verify(questDBService).truncateTable("indices_raw_d");
        verify(questDBService).importFiles("indices_raw_d");
    }

    // insertIntoHistorical
    @Test
    void insertIntoHistorical_invalidType_returnsFalse() {
        Map<String, Object> out = maintenanceService.insertIntoHistorical("xxx");
        assertThat(out).isEqualTo(falseMap);
        verifyNoInteractions(questDBService);
    }

    @Test
    void insertIntoHistorical_latestNull_returnsFalse_forDaily() {
        when(questDBService.getLatestDate(eq("historical_d"), isNull())).thenReturn(null);
        Map<String, Object> out = maintenanceService.insertIntoHistorical("d");
        assertThat(out).isEqualTo(falseMap);
    }

    @Test
    void insertIntoHistorical_buildsQueryFor_d_andExecutes() {
        when(questDBService.getLatestDate(eq("historical_d"), isNull())).thenReturn("20240101");
        when(questDBService.executeQuery(anyString())).thenReturn(Map.of("ok", true));
        ArgumentCaptor<String> qCap = ArgumentCaptor.forClass(String.class);
        maintenanceService.insertIntoHistorical("d");
        verify(questDBService).executeQuery(qCap.capture());
        String q = qCap.getValue();
        assertThat(q).contains("INSERT INTO historical_d")
                     .contains("replace(ticker, '.US', '')")
                     .contains("FROM historical_raw_d")
                     .contains("date > '20240101'")
                     .contains("ORDER BY date, time ASC");
    }

    @Test
    void insertIntoHistorical_buildsQueryFor_indices() {
        when(questDBService.getLatestDate(eq("indices_d"), isNull())).thenReturn("20231231");
        when(questDBService.executeQuery(anyString())).thenReturn(Map.of("ok", true));
        ArgumentCaptor<String> qCap = ArgumentCaptor.forClass(String.class);
        maintenanceService.insertIntoHistorical("indices_d");
        verify(questDBService).executeQuery(qCap.capture());
        String q = qCap.getValue();
        assertThat(q).contains("INSERT INTO indices_d")
                     .contains("replace(ticker, '^', '')")
                     .contains("FROM indices_raw_d");
    }

    @Test
    void insertIntoHistorical_buildsQueryFor_etf() {
        when(questDBService.getLatestDate(eq("historical_etf_d"), isNull())).thenReturn("20231201");
        when(questDBService.executeQuery(anyString())).thenReturn(Map.of("ok", true));
        ArgumentCaptor<String> qCap = ArgumentCaptor.forClass(String.class);
        maintenanceService.insertIntoHistorical("etf_d");
        verify(questDBService).executeQuery(qCap.capture());
        String q = qCap.getValue();
        assertThat(q).contains("INSERT INTO historical_etf_d")
                     .contains("replace(ticker, '.US', '')")
                     .contains("FROM historical_raw_etf_d")
                     .contains("date > '20231201'")
                     .contains("ORDER BY date, time ASC");
    }

    // insertIntoIndicator52w
    @Test
    void insertIntoIndicator52w_invalidType_returnsFalse() {
        Map<String, Object> out = maintenanceService.insertIntoIndicator52w("idx");
        assertThat(out).isEqualTo(falseMap);
    }

    @Test
    void insertIntoIndicator52w_latestNull_returnsFalse() {
        doReturn(null).when(maintenanceService).getLatestDate("indicator_d_52w", "GENERAL");
        Map<String, Object> out = maintenanceService.insertIntoIndicator52w("d");
        assertThat(out).isEqualTo(falseMap);
    }

    @Test
    void insertIntoIndicator52w_latestIsEpoch_buildsQueryWithoutFilters() {
        doReturn("19710101").when(maintenanceService).getLatestDate("indicator_d_52w", "GENERAL");
        when(questDBService.executeQuery(anyString())).thenReturn(Map.of("ok", true));
        ArgumentCaptor<String> qCap = ArgumentCaptor.forClass(String.class);
        maintenanceService.insertIntoIndicator52w("d");
        verify(questDBService).executeQuery(qCap.capture());
        String q = qCap.getValue();
        assertThat(q).contains("FROM historical_d ")
                     .doesNotContain("dateadd('d', -400")
                     .doesNotContain("WHERE date > to_date(");
    }

    @Test
    void insertIntoIndicator52w_latestAfterEpoch_addsSourceAndTargetFilters() {
        doReturn("20240101").when(maintenanceService).getLatestDate("indicator_d_52w", "GENERAL");
        when(questDBService.executeQuery(anyString())).thenReturn(Map.of("ok", true));
        ArgumentCaptor<String> qCap = ArgumentCaptor.forClass(String.class);
        maintenanceService.insertIntoIndicator52w("d");
        verify(questDBService).executeQuery(qCap.capture());
        String q = qCap.getValue();
        assertThat(q).contains("FROM historical_d WHERE date > dateadd('d', -400, to_date('20240101', 'yyyyMMdd'))")
                     .contains("FROM first_stage WHERE date > to_date('20240101', 'yyyyMMdd')");
    }

    @Test
    void insertIntoIndicator52w_forEtf_buildsQueryAgainstEtfTables() {
        doReturn("19710101").when(maintenanceService).getLatestDate("indicator_etf_52w", "GENERAL");
        when(questDBService.executeQuery(anyString())).thenReturn(Map.of("ok", true));
        ArgumentCaptor<String> qCap = ArgumentCaptor.forClass(String.class);
        maintenanceService.insertIntoIndicator52w("etf_d");
        verify(questDBService).executeQuery(qCap.capture());
        String q = qCap.getValue();
        assertThat(q).contains("FROM historical_etf_d ")
                     .contains("INSERT INTO indicator_etf_52w");
    }

    // insertIntoIndicatorMA
    @Test
    void insertIntoIndicatorMA_invalidType_returnsFalse() {
        Map<String, Object> out = maintenanceService.insertIntoIndicatorMA("x", 50, true);
        assertThat(out).isEqualTo(falseMap);
    }

    @Test
    void insertIntoIndicatorMA_truncateTrue_failure_returnsFalse() {
        when(questDBService.truncateTable("indicator_d_MA")).thenReturn(false);
        Map<String, Object> out = maintenanceService.insertIntoIndicatorMA("d", 50, true);
        assertThat(out).isEqualTo(falseMap);
    }

    @Test
    void insertIntoIndicatorMA_truncateFalse_buildsQueryAndExecutes_forEtf() {
        when(questDBService.executeQuery(anyString())).thenReturn(Map.of("ok", true));
        ArgumentCaptor<String> qCap = ArgumentCaptor.forClass(String.class);
        maintenanceService.insertIntoIndicatorMA("etf_d", 200, false);
        verify(questDBService).executeQuery(qCap.capture());
        String q = qCap.getValue();
        assertThat(q).contains("FROM historical_etf_d")
                     .contains("INSERT INTO indicator_etf_MA")
                     .contains("ROWS BETWEEN 200 PRECEDING")
                     .contains("'MA_200' AS type");
        verify(questDBService, never()).truncateTable(anyString());
    }

    @Test
    void insertIntoIndicatorMA_truncateTrue_buildsQueryAndExecutes_forDaily() {
        when(questDBService.truncateTable("indicator_d_MA")).thenReturn(true);
        when(questDBService.executeQuery(anyString())).thenReturn(Map.of("ok", true));
        ArgumentCaptor<String> qCap = ArgumentCaptor.forClass(String.class);
        maintenanceService.insertIntoIndicatorMA("d", 50, true);
        verify(questDBService).truncateTable("indicator_d_MA");
        verify(questDBService).executeQuery(qCap.capture());
        String q = qCap.getValue();
        assertThat(q).contains("FROM historical_d")
                     .contains("INSERT INTO indicator_d_MA")
                     .contains("ROWS BETWEEN 50 PRECEDING")
                     .contains("'MA_50' AS type");
    }

    // insertIntoAnalysisMA
    @Test
    void insertIntoAnalysisMA_invalidType_returnsFalse() {
        Map<String, Object> out = maintenanceService.insertIntoAnalysisMA("x");
        assertThat(out).isEqualTo(falseMap);
    }

    @Test
    void insertIntoAnalysisMA_latestNull_returnsFalse() {
        when(questDBService.getLatestDate(eq("analysis_market"), anyString())).thenReturn(null);
        Map<String, Object> out = maintenanceService.insertIntoAnalysisMA("MA");
        assertThat(out).isEqualTo(falseMap);
    }

    @Test
    void insertIntoAnalysisMA_happy_callsExecuteWithFilters() {
        when(questDBService.getLatestDate(eq("analysis_market"), anyString())).thenReturn("20240101");
        when(questDBService.executeQuery(anyString())).thenReturn(Map.of("ok", true));
        ArgumentCaptor<String> qCap = ArgumentCaptor.forClass(String.class);
        maintenanceService.insertIntoAnalysisMA("MA");
        verify(questDBService).executeQuery(qCap.capture());
        String q = qCap.getValue();
        assertThat(q).contains("type LIKE 'MA_%'")
                     .contains("AND date > to_date('20240101', 'yyyyMMdd')")
                     .contains("GROUP BY type, date ORDER BY type, date ASC");
    }

    // insertIntoIndicatorMACompare
    @Test
    void insertIntoIndicatorMACompare_invalidType_returnsFalse() {
        Map<String, Object> out = maintenanceService.insertIntoIndicatorMACompare("x", 50, 200);
        assertThat(out).isEqualTo(falseMap);
    }

    @Test
    void insertIntoIndicatorMACompare_daily_buildsQuery() {
        when(questDBService.executeQuery(anyString())).thenReturn(Map.of("ok", true));
        ArgumentCaptor<String> qCap = ArgumentCaptor.forClass(String.class);
        maintenanceService.insertIntoIndicatorMACompare("d", 50, 200);
        verify(questDBService).executeQuery(qCap.capture());
        String q = qCap.getValue();
        assertThat(q).contains("FROM indicator_d_MA i1")
                     .contains("JOIN indicator_d_MA i2")
                     .contains("i1.type = 'MA_50'")
                     .contains("i2.type = 'MA_200'")
                     .contains("INSERT INTO indicator_d_MA")
                     .contains("'MA_50_200' AS type");
    }

    @Test
    void insertIntoIndicatorMACompare_etf_buildsQuery() {
        when(questDBService.executeQuery(anyString())).thenReturn(Map.of("ok", true));
        ArgumentCaptor<String> qCap = ArgumentCaptor.forClass(String.class);
        maintenanceService.insertIntoIndicatorMACompare("etf_d", 10, 30);
        verify(questDBService).executeQuery(qCap.capture());
        String q = qCap.getValue();
        assertThat(q).contains("FROM historical_etf_d i1")
                     .contains("JOIN historical_etf_d i2")
                     .contains("INSERT INTO indicator_etf_MA")
                     .contains("'MA_10_30' AS type");
    }

    // updateAnalysisMA
    @Test
    void updateAnalysisMA_happyPath_aggregatesDurationsAndReturnsSuccess() {
        doReturn(dmlOkResult(10)).when(maintenanceService).insertIntoIndicatorMA("d", 50, true);
        doReturn(dmlOkResult(20)).when(maintenanceService).insertIntoIndicatorMA("d", 200, false);
        doReturn(dmlOkResult(30)).when(maintenanceService).insertIntoIndicatorMACompare("d", 50, 200);
        doReturn(dmlOkResult(40)).when(maintenanceService).insertIntoAnalysisMA("MA");

        Map<String, Object> out = maintenanceService.updateAnalysisMA("d", 50, 200);
        assertThat(out).containsEntry("success", Boolean.TRUE)
                       .containsEntry("duration", 100);

        verify(maintenanceService).insertIntoIndicatorMA("d", 50, true);
        verify(maintenanceService).insertIntoIndicatorMA("d", 200, false);
        verify(maintenanceService).insertIntoIndicatorMACompare("d", 50, 200);
        verify(maintenanceService).insertIntoAnalysisMA("MA");
    }

    @Test
    void updateAnalysisMA_missingResponseInFirstStep_returnsFalse() {
        doReturn(new HashMap<>()).when(maintenanceService).insertIntoIndicatorMA("d", 50, true);
        Map<String, Object> out = maintenanceService.updateAnalysisMA("d", 50, 200);
        assertThat(out).isEqualTo(falseMap);
    }

    @Test
    void updateAnalysisMA_firstStepNotOk_returnsFalse() {
        doReturn(dmlFailResult(5)).when(maintenanceService).insertIntoIndicatorMA("d", 50, true);
        Map<String, Object> out = maintenanceService.updateAnalysisMA("d", 50, 200);
        assertThat(out).isEqualTo(falseMap);
    }

    @Test
    void updateAnalysisMA_missingResponseInLaterSteps_returnsFalse() {
        doReturn(dmlOkResult(10)).when(maintenanceService).insertIntoIndicatorMA("d", 50, true);
        doReturn(new HashMap<>()).when(maintenanceService).insertIntoIndicatorMA("d", 200, false);
        Map<String, Object> out1 = maintenanceService.updateAnalysisMA("d", 50, 200);
        assertThat(out1).isEqualTo(falseMap);

        doReturn(dmlOkResult(20)).when(maintenanceService).insertIntoIndicatorMA("d", 200, false);
        doReturn(new HashMap<>()).when(maintenanceService).insertIntoIndicatorMACompare("d", 50, 200);
        Map<String, Object> out2 = maintenanceService.updateAnalysisMA("d", 50, 200);
        assertThat(out2).isEqualTo(falseMap);

        doReturn(dmlOkResult(30)).when(maintenanceService).insertIntoIndicatorMACompare("d", 50, 200);
        doReturn(new HashMap<>()).when(maintenanceService).insertIntoAnalysisMA("MA");
        Map<String, Object> out3 = maintenanceService.updateAnalysisMA("d", 50, 200);
        assertThat(out3).isEqualTo(falseMap);
    }

    // insertIntoAnalysis52w
    @Test
    void insertIntoAnalysis52w_invalidType_returnsFalse() {
        Map<String, Object> out = maintenanceService.insertIntoAnalysis52w("x");
        assertThat(out).isEqualTo(falseMap);
    }

    @Test
    void insertIntoAnalysis52w_latestNull_returnsFalse() {
        when(questDBService.getLatestDate(eq("analysis_market"), anyString())).thenReturn(null);
        Map<String, Object> out = maintenanceService.insertIntoAnalysis52w("high52w");
        assertThat(out).isEqualTo(falseMap);
    }

    @Test
    void insertIntoAnalysis52w_high52w_buildsQuery() {
        when(questDBService.getLatestDate(eq("analysis_market"), anyString())).thenReturn("20240101");
        when(questDBService.executeQuery(anyString())).thenReturn(Map.of("ok", true));
        ArgumentCaptor<String> qCap = ArgumentCaptor.forClass(String.class);
        maintenanceService.insertIntoAnalysis52w("high52w");
        verify(questDBService).executeQuery(qCap.capture());
        String q = qCap.getValue();
        assertThat(q).contains("'high52w' as 'type'")
                     .contains("FROM indicator_d_52w")
                     .contains("AND date > to_date('20240101', 'yyyyMMdd')")
                     .contains("ORDER BY type, date ASC");
    }

    @Test
    void insertIntoAnalysis52w_low52w_buildsQuery() {
        when(questDBService.getLatestDate(eq("analysis_market"), anyString())).thenReturn("20240101");
        when(questDBService.executeQuery(anyString())).thenReturn(Map.of("ok", true));
        ArgumentCaptor<String> qCap = ArgumentCaptor.forClass(String.class);
        maintenanceService.insertIntoAnalysis52w("low52w");
        verify(questDBService).executeQuery(qCap.capture());
        String q = qCap.getValue();
        assertThat(q).contains("'low52w' as 'type'")
                     .contains("FROM indicator_d_52w");
    }

    // updateAnalysis52w
    @Test
    void updateAnalysis52w_happyPath_aggregatesDurationsAndReturnsSuccess() {
        doReturn(dmlOkResult(5)).when(maintenanceService).insertIntoIndicator52w("d");
        doReturn(dmlOkResult(6)).when(maintenanceService).insertIntoAnalysis52w("high52w");
        doReturn(dmlOkResult(7)).when(maintenanceService).insertIntoAnalysis52w("low52w");
        Map<String, Object> out = maintenanceService.updateAnalysis52w("d");
        assertThat(out).containsEntry("success", Boolean.TRUE)
                       .containsEntry("duration", 18);
        verify(maintenanceService).insertIntoIndicator52w("d");
        verify(maintenanceService).insertIntoAnalysis52w("high52w");
        verify(maintenanceService).insertIntoAnalysis52w("low52w");
    }

    @Test
    void updateAnalysis52w_missingResponseInAnyStep_returnsFalse() {
        doReturn(new HashMap<>()).when(maintenanceService).insertIntoIndicator52w("d");
        assertThat(maintenanceService.updateAnalysis52w("d")).isEqualTo(falseMap);

        doReturn(dmlOkResult(5)).when(maintenanceService).insertIntoIndicator52w("d");
        doReturn(new HashMap<>()).when(maintenanceService).insertIntoAnalysis52w("high52w");
        assertThat(maintenanceService.updateAnalysis52w("d")).isEqualTo(falseMap);

        doReturn(dmlOkResult(6)).when(maintenanceService).insertIntoAnalysis52w("high52w");
        doReturn(new HashMap<>()).when(maintenanceService).insertIntoAnalysis52w("low52w");
        assertThat(maintenanceService.updateAnalysis52w("d")).isEqualTo(falseMap);
    }
}
