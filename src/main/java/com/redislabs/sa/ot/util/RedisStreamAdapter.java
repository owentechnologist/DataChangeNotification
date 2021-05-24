package com.redislabs.sa.ot.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisStreamAdapter {

    private JedisPool connectionPool;
    private String streamName;

    public RedisStreamAdapter(String streamName, JedisPool connectionPool){
        this.connectionPool=connectionPool;
        this.streamName=streamName;
    }

    public void listenToStream(MapProcessor mapProcessor){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try (Jedis streamListener =  connectionPool.getResource();){
                        String key = "";
                        List<StreamEntry> streamEntryList = null;
                        String value = "";
                        StreamEntryID nextID = new StreamEntryID();
                        System.out.println("main.kickOffStreamListenerThread: Actively Listening to Stream "+streamName);
                        Map.Entry<String, StreamEntryID> streamQuery = null;
                        while(true){
                            streamQuery = new AbstractMap.SimpleImmutableEntry<>(
                                    streamName, nextID);
                            List<Map.Entry<String, List<StreamEntry>>> streamResult =
                                    streamListener.xread(1,Long.MAX_VALUE,streamQuery);// <--  changed block from Long.MAX_VALUE
                            key = streamResult.get(0).getKey(); // name of Stream
                            streamEntryList = streamResult.get(0).getValue();
                            value = streamEntryList.get(0).toString();// entry written to stream
                            //1621843705891-0 {type2:phone={"event": "set", "key": "type2:phone", "type": "string", "value": "312-7777"}}
                            System.out.println("StreamListenerThread: received... "+key+" "+value);
                            HashMap<String,String> entry = new HashMap<String,String>();
                            entry.put(key,value);
                            mapProcessor.processMap(entry);
                            //LocalDataStore.setValue(value.substring(splitLocation));
                            nextID = new StreamEntryID(value.split(" ")[0]);
                        }
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                }
            }).start();

    }

}
