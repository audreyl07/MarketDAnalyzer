package dev.audreyl07.MDAnalyzer.controller;

import dev.audreyl07.MDAnalyzer.service.DataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

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