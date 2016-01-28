package com.niuchart.test.cube;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.niuchart.datafactory.command.NXKeyCommand;

import java.util.ArrayList;
import java.util.List;

public class NXGranularities implements NXKeyCommand{
    private String key;

    private String title;

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
    public static NXGranularities fill(JSONObject jo){
        NXGranularities o = new NXGranularities();
        if (jo.containsKey("key")) {
            o.setKey(jo.getString("key"));
        }
        if (jo.containsKey("title")) {
            o.setTitle(jo.getString("title"));
        }
        return o;
    }
    public static List<NXGranularities> fillList(JSONArray ja) {
        if (ja == null || ja.size() == 0)
            return null;
        List<NXGranularities> sqs = new ArrayList<NXGranularities>();
        for (int i = 0; i < ja.size(); i++) {
            sqs.add(fill(ja.getJSONObject(i)));
        }
        return sqs;
    }

}