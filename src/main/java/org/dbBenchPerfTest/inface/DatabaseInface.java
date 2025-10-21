package org.dbBenchPerfTest.inface;

import java.io.IOException;
import java.sql.SQLException;

public interface DatabaseInface {

    void createPartitionTable();
    void copyData() throws IOException;      // for scenario1
    void createPartIndexes() throws IOException;
//    void distinctSql() throws SQLException;      // for scenario2
//    void rowByRowInsert() throws SQLException, IOException;           // for scenario4
//    void mixedReadAndInsert() throws SQLException;       // for scenario5


}
