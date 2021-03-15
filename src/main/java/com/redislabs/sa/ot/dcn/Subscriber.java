package com.redislabs.sa.ot.dcn;

import redis.clients.jedis.JedisPubSub;

public class Subscriber extends JedisPubSub {

    @Override
    public void onMessage(String channel, String message) {
        System.out.println("Message received. Channel: "+channel+", Msg: "+ message);
        LocalDataStore.setValue(message);
    }

    @Override
    public void onPMessage(String pattern, String channel, String message) {
        System.out.println("Message received. Pattern: "+pattern+", Channel: "+channel+", Msg: "+ message);
    }

    @Override
    public void onSubscribe(String channel, int subscribedChannels) {
        System.out.println("onSubscribe called. Channel: "+channel+", subscribedChannels (count): "+subscribedChannels);
    }

    @Override
    public void onUnsubscribe(String channel, int subscribedChannels) {
        System.out.println("onUnsubscribe called. Channel: "+channel+", subscribedChannels (count): "+subscribedChannels);
    }

    @Override
    public void onPUnsubscribe(String pattern, int subscribedChannels) {
        System.out.println("onPUnsubscribe called. Channel: "+pattern+", subscribedChannels (count): "+subscribedChannels);
    }

    @Override
    public void onPSubscribe(String pattern, int subscribedChannels) {
        System.out.println("onPSubscribe called. Channel: "+pattern+", subscribedChannels (count): "+subscribedChannels);
    }
}
