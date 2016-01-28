package com.niuchart.datafactory;
import com.niuchart.datafactory.command.NXKeyCommand;
import com.niuchart.datafactory.command.NXKeyDimenCommand;
import com.niuchart.datafactory.command.NXKeyMeasureCommand;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import java.io.File;
import java.sql.*;
import java.util.*;

/**
 * SQLite生成for niuchart移动端
 */
public abstract class NXDataProducer implements MetadataCubeOperator{
    private static final Logger logger = Logger.getLogger(NXDataProducer.class);

    private Connection cubeConn;
    private Statement cubeStmt;
    private Statement cubeStmt2;

    private File baseFolder;
    private Set<String> granularityKeys;

    /**
     * 取粒度的所有粒度值
     * @param metadataId    元数据的id
     * @param cubePath		cube数据的文件绝对路径
     * @param columns		粒度值的key集合
     * @return	返回指定粒度的所有粒度值映射
     * @throws Exception
     */
    @Override
    public Map<String, List<String>> getColumnValues(String metadataId, String cubePath, List<String> columns) throws Exception {
        File cubeDbFile = new File(cubePath);
        if (!cubeDbFile.exists()) {
            throw new Exception("Metadata cube db file not found: " + cubeDbFile);
        }

        Class.forName("org.sqlite.JDBC");
        cubeConn = DriverManager.getConnection("jdbc:sqlite:" + cubeDbFile.getAbsolutePath());
        cubeConn.setAutoCommit(false);
        cubeStmt = cubeConn.createStatement();

        Map<String, List<String>> result = new HashMap<String, List<String>>();

        for (String column : columns) {
            String tablename = "CT_" + StringUtils.replace(metadataId, "-", "") + "_" + column;
            String sql = "select name from " + tablename + " order by _ID";
            List<String> values = new ArrayList<String>();
            ResultSet rs = cubeStmt.executeQuery(sql);
            while (rs.next()) {
                String v = rs.getString("name");
                if (!StringUtils.isEmpty(v)) {
                    values.add(v);
                }
            }
            result.put(column, values);
        }

        return result;
    }

    /**
     * 初始化dataset的连接或准备操作，如
     * DriverManager.getConnection,createStatement等
     * @param datasetId
     * @throws Exception
     */
    protected abstract void initDataSet(String datasetId) throws Exception;

    @Override
    public String createCube(String metadataId, String datasetId, List<? extends NXKeyDimenCommand> dimensions, List<? extends NXKeyMeasureCommand> measures,String password) throws Exception {
        //cube sqlite 打开连接与stmt
        Class.forName("org.sqlite.JDBC");
        baseFolder = generateDbFolder();
        logger.info("Metadata cube folder: " + baseFolder.getAbsolutePath());
        File dbFile = new File(baseFolder, MetadataCubeOperator.DB_FILE_NAME);
        Properties props = new Properties();
        if (password != null) {
            props.put("key", password);
        }
        cubeConn = DriverManager.getConnection("jdbc:sqlite:" + dbFile.getAbsolutePath(),props);
        cubeConn.setAutoCommit(false);
        cubeStmt = cubeConn.createStatement();
        cubeStmt2 = cubeConn.createStatement();

        //生成粒度集合
        granularityKeys = new HashSet<String>();
        for (NXKeyDimenCommand ds : dimensions) {
            for (NXKeyCommand cs : ds.getGranularities()) {
                granularityKeys.add(cs.getKey());
            }
        }

        //准备dataset
        initDataSet(datasetId);


        Map<String, Map<String, Integer>> columnIdMappingMap = new HashMap<String, Map<String, Integer>>();
        // 生成各个粒度的列表
        for (String granKey : granularityKeys) {
            Map<String, Integer> map = generateColumnTable(metadataId, granKey);
            columnIdMappingMap.put(granKey, map);
        }

        // 创建各层级汇总表
        boolean tryNextLevel = true;
        int level = 0;
        while (tryNextLevel) {
            tryNextLevel = processSummaryTable(metadataId, level, dimensions, measures, columnIdMappingMap);
            if (tryNextLevel) {
                level++;
            }
        }

        // 创建维度表
        String factTablename = "T_" + StringUtils.replace(metadataId, "-", "") + "_L" + (level - 1);
        for (NXKeyDimenCommand dimension : dimensions) {
            this.generateDimensionTables(metadataId, factTablename, dimension);
        }

        return baseFolder.getName();
    }

    /**
     * 获取某粒度的所有唯一粒度值
     * @param granKey 粒度的key
     * @return 指定粒度key的所有唯一粒度值
     * @throws Exception
     */
    protected abstract List<Object> getDataSetDistinctValueByGranKey(String granKey) throws Exception;
    /**
     * 创建粒度列表
     * @param metadataId    元数据id
     * @param columnName    列名，这里指粒度的key
     * @return
     * @throws Exception
     */
    protected Map<String, Integer> generateColumnTable(String metadataId, String columnName) throws Exception {

        String tableName = "CT_" + StringUtils.replace(metadataId, "-", "") + "_" + columnName;
		/*
		 * 列值的id映射表,key是值, value是该值在列表中的id
		 */
        Map<String, Integer> columnIdMap = new HashMap<String, Integer>();

        // 建表
        {
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE TABLE ").append(tableName);
            sb.append("( name VARCHAR(255) ");
            sb.append(", _ID INTEGER PRIMARY KEY AUTOINCREMENT");
            sb.append(")");

            logger.info("SQL for creating column table: " + sb.toString());

            cubeStmt.execute(sb.toString());
        }

        cubeConn.commit();

        // 写数据
        {
            List<Object> granValueList = getDataSetDistinctValueByGranKey(columnName);
            for (int i = 0; i < granValueList.size(); i++) {
                Object objValue = granValueList.get(i);
                StringBuilder sb = new StringBuilder();
                sb.append("INSERT INTO ").append(tableName);
                sb.append("(name) values (");
                if (objValue == null || StringUtils.isEmpty(objValue.toString())) {
                    sb.append("null");
                } else {
                    String value = StringEscapeUtils.escapeSql(objValue.toString());
                    sb.append("'").append(value).append("'");
                }
                sb.append(")");
                logger.debug("SQL for insert column table row: " + sb.toString());
                cubeStmt.execute(sb.toString());
                columnIdMap.put(objValue == null ? null : StringEscapeUtils.escapeSql(objValue.toString()), i + 1);
            }
            logger.info(granValueList.size() + " records inserted into table: " + tableName);
            cubeConn.commit();
        }

        return columnIdMap;

    }

    protected void generateDimensionTables(String metadataId, String factTablename, NXKeyDimenCommand dimension) throws Exception {
        String tableName = "DT_" + StringUtils.replace(metadataId, "-", "") + "_" + dimension.getKey();

        // 建表
        {
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE TABLE ").append(tableName);
            sb.append("(");
            for (NXKeyCommand granStru : dimension.getGranularities()) {
                sb.append(granStru.getKey()).append(" INTEGER,");
            }
            sb.append(" _ID INTEGER PRIMARY KEY AUTOINCREMENT");
            sb.append(")");

            logger.info("SQL for creating dimension table: " + sb.toString());

            cubeStmt.execute(sb.toString());
        }

        cubeConn.commit();

        // 写数据
        {

            List<String> columnNames = new ArrayList<String>();

            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("select distinct ");
            for (int i = 0; i < dimension.getGranularities().size(); i++) {
                if (i > 0) {
                    sqlBuilder.append(",");
                }
                NXKeyCommand granStru = dimension.getGranularities().get(i);
                sqlBuilder.append(granStru.getKey());
                columnNames.add(granStru.getKey());
            }
            sqlBuilder.append(" from ").append(factTablename).append(" order by _id");
            String sql = sqlBuilder.toString();
            logger.info("Query sql for dimension table: " + sql);
            ResultSet rs = cubeStmt.executeQuery(sql);

            int count = 0;

            while (rs.next()) {
                count++;
                StringBuilder sb = new StringBuilder();
                sb.append("INSERT INTO ").append(tableName);
                sb.append("(");
                sb.append(StringUtils.join(columnNames, ","));
                sb.append(") values (");

                for (int i = 0; i < columnNames.size(); i++) {
                    if (i > 0) {
                        sb.append(",");
                    }
                    String columnName = columnNames.get(i);
                    Object objValue = rs.getObject(columnName);
                    if (objValue == null || StringUtils.isEmpty(objValue.toString())) {
                        sb.append("null");
                    } else {
                        String value = StringEscapeUtils.escapeSql(objValue.toString());
                        sb.append("'").append(value).append("'");
                    }
                }

                sb.append(")");
                logger.debug("SQL for insert dimension table row: " + sb.toString());
                cubeStmt2.execute(sb.toString());
            }
            logger.info(count + " records inserted into table: " + tableName);
            cubeConn.commit();
        }

    }

    abstract protected List<Map<String, Object>> queryLevelDataFromDataSet(List<String> selectFields, List<String> groupFields, List<String> dimensionColumnNames, List<String> measureColumnNames, boolean isGroupNotNeeded) throws SQLException;
    /**
     * 创建汇总表
     * <p/>
     * 表名: 100101
     */
    protected boolean processSummaryTable(String metadataId, int level, List<? extends NXKeyDimenCommand> dimensions, List<? extends NXKeyMeasureCommand> measures,
                                          Map<String, Map<String, Integer>> columnIdMappingMap) throws Exception {

        List<NXKeyCommand> columns = new ArrayList<NXKeyCommand>();
        List<String> dimensionColumnNames = new ArrayList<String>();
        List<String> measureColumnNames = new ArrayList<String>();

        // 是否有这个层级
        boolean hasLevel = false;
        boolean isGroupNotNeeded = true;
        for (NXKeyDimenCommand dimension : dimensions) {
            List<? extends NXKeyCommand> grans = dimension.getGranularities();
            if (level < grans.size()) {
                hasLevel = true;
            }
            if (grans.size() > level + 1) {
                isGroupNotNeeded = false;
            }
            for (int i = 0; i < Math.min(level + 1, grans.size()); i++) {
                NXKeyCommand gran = grans.get(i);
                columns.add(gran);
            }
        }

        if (!hasLevel) {
            return false;
        }

        // select字段
        List<String> selectFields = new ArrayList<String>();
        // 需要group的字段
        List<String> groupFields = new ArrayList<String>();

        for (NXKeyCommand column : columns) {
            selectFields.add(column.getKey());
            groupFields.add(column.getKey());
            dimensionColumnNames.add(column.getKey());
        }
        for (NXKeyMeasureCommand measure : measures) {
            if (measure.getIsCreated() != null && measure.getIsCreated()) {
                selectFields.add(" NULL as " + measure.getKey() + " ");
                continue;
            }
            if (!isGroupNotNeeded) {
                String funcString=null;
                Integer calculationType = measure.getCalculationType();
                switch (calculationType) {
                    case 2:
                        funcString=" avg";
                        break;
                    case 1:
                    default:
                        funcString=" sum";
                        break;
                }
                selectFields.add(funcString+"(" + measure.getKey() + ") as " + measure.getKey() + " ");
            } else {
                selectFields.add(measure.getKey());
            }
            measureColumnNames.add(measure.getKey());
        }

        // 建表
        String tableName = "T_" + StringUtils.replace(metadataId, "-", "") + "_L" + level;
        createTable(tableName, columns, dimensions, measures, level);
        cubeConn.commit();

        //查数据
        List<Map<String, Object>> dataSetList = queryLevelDataFromDataSet(selectFields, groupFields, dimensionColumnNames, measureColumnNames, isGroupNotNeeded);
        //插数据
        List<String> columnNames = new ArrayList<String>();
        columnNames.addAll(dimensionColumnNames);
        columnNames.addAll(measureColumnNames);
        PreparedStatement pareStmt = createPreparestatementInsert(cubeConn, tableName, columnNames);

        for (int i = 0; i < dataSetList.size(); i++) {
            //一行数据的映身寸
            Map<String, Object> eachObj = dataSetList.get(i);
            int columnIndex = 1;
            for (String columnName : dimensionColumnNames) {
                Object valueObj = eachObj.get(columnName);
                if (valueObj instanceof String) {
                    String value = (String) valueObj;
                    Map<String, Integer> idMap = columnIdMappingMap.get(columnName);
                    Integer id = idMap.get(value == null ? null : StringEscapeUtils.escapeSql(value));
                    pareStmt.setInt(columnIndex, id);
                    columnIndex++;
                } else {
                    throw new Exception("Granularity value must be String type");
                }
            }

            for(String columnName : measureColumnNames){
                Object valueObj = eachObj.get(columnName);
                if (valueObj instanceof Double) {
                    Double value = (Double) valueObj;
                    pareStmt.setDouble(columnIndex, value);
                    columnIndex++;
                } else {
                    throw new Exception("Measure value must be Double type");
                }
            }
            pareStmt.addBatch();
            if (i >= 50000 && i % 50000 == 0) {
                logger.info((i + 1) + " records inserted into table: " + tableName);
                pareStmt.executeBatch();
                cubeConn.commit();
            }
        }

        logger.info(dataSetList.size() + " records inserted into table: " + tableName);
        pareStmt.executeBatch();
        cubeConn.commit();

        return true;

    }

    protected PreparedStatement createPreparestatementInsert(Connection conn, String tableName, List<String> columnNames) throws Exception {

        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(tableName);

        sb.append("(");
        for (int m = 0; m < columnNames.size(); m++) {
            if (m > 0) {
                sb.append(",");
            }
            sb.append(columnNames.get(m));
        }
        sb.append(")");

        sb.append(" values(");
        for (int m = 0; m < columnNames.size(); m++) {
            if (m > 0) {
                sb.append(",");
            }
            sb.append("?");

        }
        sb.append(")");
        logger.debug("SQL for insert metadata dataset row: " + sb.toString());
        return conn.prepareStatement(sb.toString());

    }

    /**
     * 根据维度、粒度、度量以及层级系数产生表
     * @param tableName 表名
     * @param grans 粒度集合
     * @param dimensions 维度集合
     * @param measures 度量集合
     * @param level 层级系数
     * @throws Exception
     */
    protected void createTable(String tableName, List<NXKeyCommand> grans, List<? extends NXKeyDimenCommand> dimensions, List<? extends NXKeyMeasureCommand> measures, int level) throws Exception {
        // 建表
        List<String> granColumnNames = new ArrayList<String>();

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(tableName).append("(");

        for (int i = 0; i < grans.size(); i++) {
            NXKeyCommand column = grans.get(i);
            String columnName = column.getKey();
            if ("id".equalsIgnoreCase(columnName)) {
                continue;
            }
            if (i > 0) {
                sb.append(",");
            }
            sb.append(columnName).append(" ");
            sb.append("INTEGER");

            granColumnNames.add(columnName);
        }

        for (NXKeyCommand measure : measures) {
            sb.append(",");
            sb.append(measure.getKey()).append(" ").append("DOUBLE");
        }

        sb.append(", _ID INTEGER PRIMARY KEY AUTOINCREMENT");

        sb.append(")");

        logger.info("SQL for creating metadata dataset table: " + sb.toString());

        cubeStmt.execute(sb.toString());

        {
            String sql = "CREATE INDEX IDX_" + tableName + "  ON " + tableName + " (" + StringUtils.join(granColumnNames, ",") + ")";
            logger.info("Creating index: " + sql);
            cubeStmt.execute(sql);
        }

        // 如果超过2个维度, 每个维度的第一个粒度, 建立联合索引
        int idxIndex = 0;
        if (dimensions.size() > 2) {
            for (int i = 0; i < dimensions.size() - 1; i++) {
                int m = i + 1;

                while (m < dimensions.size()) {
                    String idxColumns = dimensions.get(i).getGranularities().get(0).getKey() + ","
                            + dimensions.get(m).getGranularities().get(0).getKey();
                    String sql = "CREATE INDEX IDX_" + tableName + "_" + (idxIndex++) + "  ON " + tableName + " (" + idxColumns + ")";
                    logger.info("Creating index: " + sql);
                    cubeStmt.execute(sql);
                    m++;
                }
            }
        }

    }

    protected void insertRow(String tableName, List<NXKeyCommand> columns, List<NXKeyCommand> measures, ResultSet rs, Map<String, Map<String, Integer>> columnIdMappingMap) throws Exception {

        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(tableName);

        sb.append("(");
        for (int m = 0; m < columns.size(); m++) {
            if (m > 0) {
                sb.append(",");
            }
            sb.append(columns.get(m).getKey());
        }
        for (NXKeyCommand measure : measures) {
            sb.append(",");
            sb.append(measure.getKey());
        }
        sb.append(")");

        sb.append(" values(");

        for (int m = 0; m < columns.size(); m++) {
            if (m > 0) {
                sb.append(",");
            }
            String columnName = columns.get(m).getKey();
            Object objValue = rs.getObject(columnName);
            Map<String, Integer> idMap = columnIdMappingMap.get(columnName);
            Integer id = idMap.get(objValue == null ? null : StringEscapeUtils.escapeSql(objValue.toString()));
            sb.append(id);
        }
        for (NXKeyCommand measure : measures) {
            sb.append(",");
            Double objValue = rs.getDouble(measure.getKey());
            sb.append(objValue);
        }
        sb.append(")");
        logger.debug("SQL for insert metadata dataset row: " + sb.toString());
        cubeStmt.execute(sb.toString());

    }

    @Override
    public void close() {
        try {
            cubeStmt.close();
        } catch (Exception e) {
        }
        try {
            cubeStmt2.close();
        } catch (Exception e) {
        }

        try {
            cubeConn.close();
        } catch (Exception e) {
        }
    }

    @Override
    public void clean(String metadataId, String cubeId) throws Exception {
        File file = new File(getTargetDatabaseFolderPath(), cubeId);
        FileUtils.deleteQuietly(file);
    }

    @Override
    public void closeAndClean() {
        close();
        FileUtils.deleteQuietly(baseFolder);
    }


    private synchronized File generateDbFolder() {
        File dbFolder = null;
        while (!(dbFolder = internalGenerateDbFolder()).exists()) {
            break;
        }
        dbFolder.mkdirs();
        return dbFolder;
    }

    private File internalGenerateDbFolder() {
        return new File(getTargetDatabaseFolderPath());
    }

    /**
     * SQLite文件产生的目标文件夹路径
     * @return
     */
    public abstract String getTargetDatabaseFolderPath();
}
