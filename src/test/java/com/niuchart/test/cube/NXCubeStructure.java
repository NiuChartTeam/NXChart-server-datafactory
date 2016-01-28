package com.niuchart.test.cube;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;
public class NXCubeStructure {
    private String dataSetId;

    private String db;

    private String encrypt;

    private List<NXDimensions> dimensions ;

    private List<NXMeasures> measures ;

    public void setDataSetId(String dataSetId){
        this.dataSetId = dataSetId;
    }
    public String getDataSetId(){
        return this.dataSetId;
    }
    public void setDb(String db){
        this.db = db;
    }
    public String getDb(){
        return this.db;
    }
    public void setEncrypt(String encrypt){
        this.encrypt = encrypt;
    }
    public String getEncrypt(){
        return this.encrypt;
    }
    public void setDimensions(List<NXDimensions> dimensions){
        this.dimensions = dimensions;
    }
    public List<NXDimensions> getDimensions(){
        return this.dimensions;
    }
    public void setMeasures(List<NXMeasures> measures){
        this.measures = measures;
    }
    public List<NXMeasures> getMeasures(){
        return this.measures;
    }
    public static NXCubeStructure fill(JSONObject jo){
        NXCubeStructure o = new NXCubeStructure();
        if (jo.containsKey("dataSetId")) {
            o.setDataSetId(jo.getString("dataSetId"));
        }
        if (jo.containsKey("db")) {
            o.setDb(jo.getString("db"));
        }
        if (jo.containsKey("encrypt")) {
            o.setEncrypt(jo.getString("encrypt"));
        }
        if (jo.containsKey("dimensions")) {
            o.setDimensions(NXDimensions.fillList(jo.getJSONArray("dimensions")));
        }
        if (jo.containsKey("measures")) {
            o.setMeasures(NXMeasures.fillList(jo.getJSONArray("measures")));
        }
        return o;
    }
    public static List<NXCubeStructure> fillList(JSONArray ja) {
        if (ja == null || ja.size() == 0)
            return null;
        List<NXCubeStructure> sqs = new ArrayList<NXCubeStructure>();
        for (int i = 0; i < ja.size(); i++) {
            sqs.add(fill(ja.getJSONObject(i)));
        }
        return sqs;
    }

}