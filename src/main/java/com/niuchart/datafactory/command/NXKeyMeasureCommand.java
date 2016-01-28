package com.niuchart.datafactory.command;

/**
 * Created by linxiaolong on 16/1/14.
 */
public interface NXKeyMeasureCommand extends NXKeyCommand{
    /**
     * 是否是用度量之间的关系创造的度量
     * @return
     */
    Boolean getIsCreated();

    /**
     * 聚合类型,1为sum,2为avg
     * @return
     */
    Integer getCalculationType();

}
