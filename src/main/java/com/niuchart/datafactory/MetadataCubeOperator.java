package com.niuchart.datafactory;

import java.util.List;
import java.util.Map;

import com.niuchart.datafactory.command.*;

public interface MetadataCubeOperator {
	/**
	 * 生成的db的名字
	 * @return
	 */
	String getCubeGenerateDbName();

	/**
	 * 数据源的表名
	 * @return
	 */
	String getDataSetTableName();

	/**
	 * 清除数据
	 */
	void closeAndClean();

	/**
	 * 关闭
	 */
	void close();

	/**
	 * 删除相关资源
	 * @param metadataId
	 * @param cubeId
	 * @throws Exception
	 */
	void clean(String metadataId, String cubeId) throws Exception;

	/**
	 * 取粒度的所有粒度值
	 * @param metadataId    元数据的id
	 * @param cubePath		cube数据的文件绝对路径
	 * @param columns		粒度值的key集合
	 * @return	返回指定粒度的所有粒度值映射
	 * @throws Exception
	 */
	Map<String, List<String>> getColumnValues(String metadataId, String cubePath, List<String> columns) throws Exception;
	


	/**
	 * 根据dataset创建cube
	 * @param metadataId	生成的元数据id
	 * @param datasetId		源数据id
	 * @param dimensions	维度
	 * @param measures	度量
	 * @param password	生成的sqlite层级数据库是否加密,加密是将此值置为非空字符串
	 * @return
	 * @throws Exception
	 */
	String createCube(String metadataId, String datasetId, List<? extends NXKeyDimenCommand> dimensions, List<? extends NXKeyMeasureCommand> measures,String password) throws Exception;

}
