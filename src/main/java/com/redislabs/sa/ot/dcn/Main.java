package com.redislabs.sa.ot.dcn;

import com.redislabs.sa.ot.util.JedisConnectionFactory;
import com.redislabs.sa.ot.util.MapProcessor;
import com.redislabs.sa.ot.util.RedisStreamAdapter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
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

import json
# GB is the GearBuilder (a factory for gears)
s_gear = GB( desc = "SEND event of type2:* in STREAM DATA_UPDATES" )
s_gear.foreach(
    lambda x: execute('XADD', "X:DATA_UPDATES", "*", x['key'], json.dumps(x))
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


    private static final JedisPool redisConnectionPool = JedisConnectionFactory.getInstance().getJedisPool();
    //static String DATA_UPDATES_STREAM = "X:BEST_MATCHED_CITY_NAMES_BY_SEARCH";
    static String DATA_UPDATES_STREAM = "X:DATA_UPDATES";

    public static void main(String[] args){
        String channel  = "LAST_OP";
        if(args.length<1) {
            System.out.println("Please pass this program the name of the pub/sub channel you wish to subscribe to.\n" +
                    "  ex: LAST_OP\n" +
                    "Please also (as your second argument) pass this program the name of the stream you wish to repeatedly listen to \n" +
                    "  ex: DATA_UPDATES");
            System.out.println("\tDefaulting to: LAST_OP and "+DATA_UPDATES_STREAM);
        }else{
            channel = args[0];
            DATA_UPDATES_STREAM = args[1];
        }
        Main main = new Main();
        RedisStreamAdapter streamAdapter = new RedisStreamAdapter(DATA_UPDATES_STREAM,redisConnectionPool);
        streamAdapter.listenToStream(new DataUpdateEventMapProcessor());

        for(int x=0;x<360;x++){
            try{
                Thread.sleep((1000+(x*3000)%20000));
            }catch(Throwable t){}
            for(String i : LocalDataStore.getDataStore().keySet()){
                System.out.println("Simulated client Queries Java Service with getValue("+i+" ) -->\t"+LocalDataStore.getDataStore().get(i));
            }
        }
    }

}