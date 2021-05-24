package com.redislabs.sa.ot.dcn;

import com.jayway.jsonpath.JsonPath;

import java.util.HashMap;
import java.util.InvalidPropertiesFormatException;
import java.util.Map;

public class LocalDataStore {
    static Map<String,String> dataStore = new HashMap<String,String>();


    public static void setKeyValue(String key, String value){
        if(null == value){
            if(dataStore.containsKey(key)) {
                dataStore.remove(key);
            }
        }else {
            dataStore.put(key, value);
        }
    }

    //At first assume the type coming in is a map containing key and value
    public static void setValue( String payload ){
        System.out.println("LocalDataStore.setValue() called with: "+payload);
        //{"event": "set", "key": "type2:event22", "type": "string", "value": "can you hear me?"}  2021-03-13 23:19:51.837626
        String key="";
        String value="";
        try{
            key = JsonPath.read(payload, "$.key");
            value = JsonPath.read(payload, "$.value");
            setKeyValue(key,value);
        }catch(Throwable e) { // guessing that the alternate use of this program is happening so manually parse the strings...
            for (String n : payload.split(",")){
                key = n.split("=")[0];
                value = n.split("=")[1];
                setKeyValue(key, value);
            }
        }
    }

    public static Map<String,String> getDataStore(){
        return dataStore;
    }

    public static String getValue(String key){
        return dataStore.get(key);
    }
}
