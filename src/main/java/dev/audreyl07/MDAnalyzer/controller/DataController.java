package dev.audreyl07.MDAnalyzer.controller;

import dev.audreyl07.MDAnalyzer.service.DataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * REST controller exposing data retrieval endpoints for market datasets.
 *
 * Endpoints:
 * - GET /{dataType}/{resultType}/{symbol}: Retrieve time-series data for a given symbol
 *   - dataType: "stock" | "index" | "market" (market returns analysis series)
 *   - resultType: "single" (default) | "full"
 *   - symbol: ticker symbol, e.g., AAPL, ^GSPC
 */
@CrossOrigin(origins = "http://localhost:1234")
@RestController
@RequestMapping("")
public class DataController {

    @Autowired
    DataService dataService;

    @GetMapping(value = "/{dataType}/{resultType}/{symbol}")
    public ResponseEntity<Object> getData(@PathVariable String dataType, @PathVariable String resultType, @PathVariable String symbol) {
        System.out.println("dataType:" + dataType);
        System.out.println("resultType:" + resultType);
        System.out.println("symbol:" + symbol);
        if ("market".equalsIgnoreCase(dataType)) {
            return ResponseEntity.ok().body(dataService.getAnalysis(symbol));
        }
        return ResponseEntity.ok().body(dataService.getData(dataType, resultType, symbol));
    }
}