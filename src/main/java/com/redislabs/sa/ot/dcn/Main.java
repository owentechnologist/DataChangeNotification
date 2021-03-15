package com.redislabs.sa.ot.dcn;

import com.redislabs.sa.ot.util.JedisConnectionFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

// used this post as a code reference:
//https://basri.dev/posts/2012-06-20-a-simple-jedis-publish-subscribe-example/
/*
this code expects that RedisGears is in place and that the following gear has been
registered:  (you can use RedisInsight to register the gear)
(python-code-for-gear follows)

import datetime
import json
# GB is the GearBuilder (a factory for gears)
s_gear = GB( desc = "SEND event of type2:* in STREAM DATA_UPDATES" )
s_gear.foreach(
    lambda x: execute('XADD', "DATA_UPDATES", "*", x['key'], json.dumps(x)+"  "+
    datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))
)
s_gear.register(
    'type2:*',
    mode='sync',
    readValue=True
    #NB: mode='sync' makes sure the event isn’t ignored
    #readValue=True shows the value of the key
    #readValue=False shows just the key and the operation
)
 */

public class Main {

    private static final Jedis redisEventListener = JedisConnectionFactory.getInstance().getJedisPool().getResource();

    public static void main(String[] args){
        String channel  = "LAST_OP";
        String streamName = "DATA_UPDATES";
        if(args.length<1) {
            System.out.println("Please pass this program the name of the pub/sub channel you wish to subscribe to.\n" +
                    "  ex: LAST_OP\n" +
                    "Please also (as your second argument) pass this program the name of the stream you wish to repeatedly listen to \n" +
                    "  ex: DATA_UPDATES");
            System.out.println("\tDefaulting to: LAST_OP and DATA_UPDATES");
        }else{
            channel = args[0];
            streamName = args[1];
        }
        Main main = new Main();
        //main.kickOffSubscriptionThread(channel); //[this is for pub/sub  - a weaker option]
        main.kickOffStreamListenerThread(streamName); //[this uses streams - better version]
        for(int x=0;x<360;x++){
            try{
                Thread.sleep(4000);
            }catch(Throwable t){}
            for(String i : LocalDataStore.getDataStore().keySet()){
                System.out.println("Main loop checking local datastore: "+i+"\t"+LocalDataStore.getDataStore().get(i));
            }
        }
    }

    private void kickOffStreamListenerThread(String streamName){
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
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
                                redisEventListener.xread(1,3600000,streamQuery);
                        key = streamResult.get(0).getKey();
                        streamEntryList = streamResult.get(0).getValue();
                        value = streamEntryList.get(0).toString();
                        System.out.println("main.kickOffStreamListenerThread: received... "+key+" "+value);
                        int splitLocation = value.indexOf(" {");
                        LocalDataStore.setValue(value.substring(splitLocation));
                        nextID = new StreamEntryID(value.split(" ")[0]);
                    }
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private void kickOffSubscriptionThread(String channel){
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    System.out.println("Subscribing to "+channel+". This thread will be blocked.");
                    redisEventListener.subscribe(new Subscriber(),channel );
                    System.out.println("Subscription ended.");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}