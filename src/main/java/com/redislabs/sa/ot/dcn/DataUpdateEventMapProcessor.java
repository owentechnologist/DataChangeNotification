package com.redislabs.sa.ot.dcn;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.redislabs.sa.ot.util.MapProcessor;

import java.util.Map;

public class DataUpdateEventMapProcessor implements MapProcessor {
    @Override
    public void processMap(Map<String, String> map) {
        String raw = map.get(map.keySet().toArray()[0]);
        System.out.println("DataUpdateEventMapProcessor(): "+raw);
        if(raw.contains(new StringBuffer("\"type\": \"list\","))){
            int splitLocation = raw.indexOf(" {");
            String listName = raw.substring(splitLocation + 2);// no need to keep the leading space or {
            listName = listName.split("=")[0]; // grab the name of the list
            splitLocation = raw.indexOf("value\":");
            String rSide = raw.substring(splitLocation + 7); // no need to keep the value":
            String kvs = rSide.substring(0, rSide.length() - 2); // trim off the trailing }} so we now have just values framed in [ ]
            LocalDataStore.setKeyValue(listName, kvs);
        }else {
            try {
                for (String k : map.keySet()) {
                    String temp = map.get(k);
                    //1621843705891-0 {type2:phone={"event": "set", "key": "type2:phone", "type": "string", "value": "312-7777"}}
                    //we don't care about the timestamp so we cut it out
                    int splitLocation = temp.indexOf(" {");
                    String rSide = temp.substring(splitLocation + 2); // no need to keep the leading space or {
                    String json = rSide.split("=")[1]; // capture the data stored after the = sign
                    json.substring(0, json.length() - 1); // trim off the trailing } so we now have valid JSON
                    String key = JsonPath.read(json, "$.key");
                    String value = JsonPath.read(json, "$.value");
                    LocalDataStore.setKeyValue(key, value);
                }
            } catch (PathNotFoundException pnf) {
                // could be our event looks like this instead:
                //1621847672478-0 {originalCityName=cokwitlum, requestID=PM_UID76863937290053, bestMatch=Coquitlam}
                int splitLocation = raw.indexOf(" {");
                String rSide = raw.substring(splitLocation + 2); // no need to keep the leading space or {
                String kvs = rSide.substring(0, rSide.length() - 1); // trim off the trailing } so we now have just key,value pairs
                LocalDataStore.setValue(kvs);
            } catch (ClassCastException cce) {
                // could be our event looks like this instead: (Hash type)
                //1621847941565-0 {type2:hash1={"event": "hset", "key": "type2:hash1", "type": "hash", "value": {"name": "bob", "phone": "212-555-1213"}}}
                int splitLocation = raw.indexOf(" {");
                String hashPrefix = raw.substring(splitLocation + 2);// no need to keep the leading space or {
                hashPrefix = hashPrefix.split("=")[0]; // grab the name of the hash to prepend to the names of the nested variables
                hashPrefix = hashPrefix + ":"; // since we will be prepending this as a namespace (hash) indicator
                splitLocation = raw.indexOf("value");
                String rSide = raw.substring(splitLocation + 7); // no need to keep the value":
                String kvs = rSide.substring(0, rSide.length() - 3); // trim off the trailing }}} so we now have just key,value pairs
                kvs = kvs.replaceAll("\": ", "="); // replace any ":  with =
                //System.out.println(kvs);
                kvs = kvs.replaceAll("\"", ""); // remove any remaining "
                //System.out.println(kvs);
                kvs = kvs.replaceAll("\\{", hashPrefix); // prepend the first found keyname
                //System.out.println(kvs);
                kvs = kvs.replaceAll(", ", ", " + hashPrefix); // prepend any other keynames
                //System.out.println(kvs);
                //type2:hash1:name=bob, type2:hash1:phone=212-555-1213
                LocalDataStore.setValue(kvs);
            }
        }
    }
}

