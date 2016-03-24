package com.niuchart.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.niuchart.test.cube.NXCubeStructure;
import com.niuchart.test.factory.NXProducerMySQLDataSource;
import com.niuchart.test.mock.NXDataSetMockMySQLGenerator;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

/**
 * Created by linxiaolong on 16/1/14.
 */
public class NXDataFactoryMySQLDataSourceTest {
    private static final String TARGET_PATH = "target/report/";
    private String mMetaDataId;
    NXCubeStructure mCube;
    /**
     * 模拟数据源，准备数据
     * @throws IOException
     */
    @Before
    public void prepareDataSet() throws Exception {
        //清理一下
        File path = new File(TARGET_PATH);
        if (path.exists()) {
            FileUtils.forceDelete(path);
        }
        //新的metaData id
        mMetaDataId = UUID.randomUUID().toString();
        //NXCubeStructure生成
        {
            InputStream inputStream = getClass().getResourceAsStream("/metaData.json");
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            IOUtils.copy(inputStream, outputStream);
            String metaData = new String(outputStream.toByteArray());
            IOUtils.closeQuietly(inputStream);
            IOUtils.closeQuietly(outputStream);
            JSONObject metaObj = JSONObject.parseObject(metaData);
            mCube =NXCubeStructure.fill(metaObj);
        }
        //生成模拟数据
        {
            //从json中取出变成jsonArray
            InputStream inputStream = getClass().getResourceAsStream("/example.json");
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            IOUtils.copy(inputStream, outputStream);
            String exampleString = new String(outputStream.toByteArray());
            IOUtils.closeQuietly(inputStream);
            IOUtils.closeQuietly(outputStream);
            JSONArray objects = JSONArray.parseArray(exampleString);
            //生成mysql dataset mock作为模拟的数据源
            NXDataSetMockMySQLGenerator mock = new NXDataSetMockMySQLGenerator();
            mock.createTable(mCube.getDataSetId(), mCube.getDimensions(), mCube.getMeasures());
            mock.insertData(objects);
            mock.commit();
            mock.close();
        }

    }

    @Test
    public void generateSQLiteFile() throws Exception {
        //生成取终供sdk使用的层级数据库，默认放在target目录
        NXProducerMySQLDataSource productor = new NXProducerMySQLDataSource(){
            @Override
            public String getTargetDatabaseFolderPath() {
                return TARGET_PATH;
            }
        };
        productor.createCube(mMetaDataId, mCube.getDataSetId(), mCube.getDimensions(), mCube.getMeasures(), mCube.getEncrypt());
    }
}
