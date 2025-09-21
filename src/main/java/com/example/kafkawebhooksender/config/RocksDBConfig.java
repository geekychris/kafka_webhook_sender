package com.example.kafkawebhooksender.config;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import jakarta.annotation.PreDestroy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Configuration
public class RocksDBConfig {
    
    @Value("${rocksdb.path:./data/rocksdb}")
    private String rocksDBPath;
    
    private RocksDB rocksDB;
    private Options options;
    
    @Bean
    public RocksDB rocksDB() throws RocksDBException {
        RocksDB.loadLibrary();
        
        options = new Options();
        options.setCreateIfMissing(true);
        options.setMaxBackgroundCompactions(10);
        options.setMaxBackgroundFlushes(10);
        options.setMaxOpenFiles(1000);
        options.setWriteBufferSize(64 * 1024 * 1024); // 64MB
        options.setMaxWriteBufferNumber(3);
        options.setTargetFileSizeBase(64 * 1024 * 1024); // 64MB
        
        try {
            Path dbPath = Paths.get(rocksDBPath);
            Files.createDirectories(dbPath);
            rocksDB = RocksDB.open(options, dbPath.toString());
            return rocksDB;
        } catch (Exception e) {
            throw new RocksDBException("Failed to initialize RocksDB: " + e.getMessage());
        }
    }
    
    @PreDestroy
    public void cleanup() {
        if (rocksDB != null) {
            rocksDB.close();
        }
        if (options != null) {
            options.close();
        }
    }
}