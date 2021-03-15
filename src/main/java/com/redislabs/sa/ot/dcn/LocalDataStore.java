package com.redislabs.sa.ot.dcn;

import com.jayway.jsonpath.JsonPath;

import java.util.HashMap;
import java.util.Map;

public class LocalDataStore {
    static Map<String,String> dataStore = new HashMap<String,String>();

    //String pageName = JsonPath.read(yourJsonString, "$.pageInfo.pageName");
    //String pagePic = JsonPath.read(yourJsonString, "$.pageInfo.pagePic");
    //String post_id = JsonPath.read(yourJsonString, "$.pagePosts[0].post_id");
    public static void setValue( String payload ){
        //{"event": "set", "key": "updates:event22", "type": "string", "value": "can you hear me?"}  2021-03-13 23:19:51.837626
        String key = JsonPath.read(payload, "$.key");
        String value = JsonPath.read(payload, "$.value");
        if(null == value){
            dataStore.remove(key);
        }else {
            dataStore.put(key, value);
        }
    }

    public static Map<String,String> getDataStore(){
        return dataStore;
    }

    public static String getValue(String key){
        return dataStore.get(key);
    }
}
