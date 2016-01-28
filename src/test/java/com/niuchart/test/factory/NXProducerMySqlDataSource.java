package com.niuchart.test.factory;

import com.niuchart.datafactory.NXDataProducer;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Created by linxiaolong on 16/1/15.
 */
public class NXProducerMySqlDataSource extends NXDataProducer {
    @Override
    protected void initDataSet(String datasetId) throws Exception {

    }

    @Override
    protected List<Object> getDataSetDistinctValueByGranKey(String granKey) throws Exception {
        return null;
    }

    @Override
    protected List<Map<String, Object>> queryLevelDataFromDataSet(List<String> selectFields, List<String> groupFields, List<String> dimensionColumnNames, List<String> measureColumnNames, boolean isGroupNotNeeded) throws SQLException {
        return null;
    }

    @Override
    public String getTargetDatabaseFolderPath() {
        return null;
    }
}
