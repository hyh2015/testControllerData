package org.dbBenchPerfTest.dataBase;

import org.dbBenchPerfTest.TestConfig;
import org.dbBenchPerfTest.inface.DatabaseInface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class VastbaseDatabase implements DatabaseInface {

    private static final Logger logger = LoggerFactory.getLogger(VastbaseDatabase.class);

    private final TestConfig config;

    public VastbaseDatabase(TestConfig config) {
        this.config = config;
    }


    @Override
    public void createPartitionTable() {

    }

    @Override
    public void copyData() throws IOException {

    }

    @Override
    public void createPartIndexes() throws IOException {

    }
}
