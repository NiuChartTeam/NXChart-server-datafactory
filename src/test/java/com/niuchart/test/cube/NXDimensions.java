package com.niuchart.test.cube;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.niuchart.datafactory.command.NXKeyDimenCommand;

import java.util.ArrayList;
import java.util.List;
public class NXDimensions implements NXKeyDimenCommand{
    private List<NXGranularities> granularities ;

    private String key;

    private String title;

    public void setGranularities(List<NXGranularities> granularities){
        this.granularities = granularities;
    }
    public List<NXGranularities> getGranularities(){
        return this.granularities;
    }
    public void setKey(String key){
        this.key = key;
    }
    public String getKey(){
        return this.key;
    }
    public void setTitle(String title){
        this.title = title;
    }
    public String getTitle(){
        return this.title;
    }
    public static NXDimensions fill(JSONObject jo){
        NXDimensions o = new NXDimensions();
        if (jo.containsKey("granularities")) {
            o.setGranularities(NXGranularities.fillList(jo.getJSONArray("granularities")));
        }
        if (jo.containsKey("key")) {
            o.setKey(jo.getString("key"));
        }
        if (jo.containsKey("title")) {
            o.setTitle(jo.getString("title"));
        }
        return o;
    }
    public static List<NXDimensions> fillList(JSONArray ja) {
        if (ja == null || ja.size() == 0)
            return null;
        List<NXDimensions> sqs = new ArrayList<NXDimensions>();
        for (int i = 0; i < ja.size(); i++) {
            sqs.add(fill(ja.getJSONObject(i)));
        }
        return sqs;
    }

}