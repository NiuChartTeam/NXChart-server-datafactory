package com.niuchart.test.cube;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.niuchart.datafactory.command.NXKeyMeasureCommand;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class NXMeasures implements NXKeyMeasureCommand {
    private static final Logger logger = Logger.getLogger(NXKeyMeasureCommand.class);
    private String key;
    private String title;
    /**
     * "SUM", "AVG", "MIN", "MAX", or "COUNT". Default SUM
     */
    String mAggregator;
    //是否是用公式计算的
    boolean mIsCreated;

    public enum NXCalculationType {
        SUM(1),
        AVG(2);
        int mValue;

        NXCalculationType(int value) {
            mValue = value;
        }
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getKey() {
        return this.key;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getTitle() {
        return this.title;
    }

    @Override
    public Boolean getIsCreated() {
        return isCreated();
    }

    @Override
    public Integer getCalculationType() {
        try {
            NXCalculationType calculationType = NXCalculationType.valueOf(mAggregator);
            return calculationType.mValue;
        } catch (Exception e) {
            logger.error(e.getMessage());
            return NXCalculationType.SUM.mValue;
        }
    }

    public String getAggregator() {
        return mAggregator;
    }

    public void setAggregator(String aggregator) {
        mAggregator = aggregator;
    }

    public boolean isCreated() {
        return mIsCreated;
    }

    public void setIsCreated(boolean isCreated) {
        mIsCreated = isCreated;
    }

    public static NXMeasures fill(JSONObject jo) {
        NXMeasures o = new NXMeasures();
        if (jo.containsKey("key")) {
            o.setKey(jo.getString("key"));
        }
        if (jo.containsKey("title")) {
            o.setTitle(jo.getString("title"));
        }
        if (jo.containsKey("isCreated")) {
            o.setIsCreated(jo.getBoolean("isCreated"));
        }
        if (jo.containsKey("aggregator")) {
            o.setAggregator(jo.getString("aggregator"));
        }
        return o;
    }

    public static List<NXMeasures> fillList(JSONArray ja) {
        if (ja == null || ja.size() == 0)
            return null;
        List<NXMeasures> sqs = new ArrayList<NXMeasures>();
        for (int i = 0; i < ja.size(); i++) {
            sqs.add(fill(ja.getJSONObject(i)));
        }
        return sqs;
    }
}