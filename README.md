# DataChangeNotification
A hack in Java/Jedis that demonstrates how Gears can notify a client of changes to keys within Redis

Possible Condition:

Billions of requests per day are hitting Redis asking for the latest copies of a million bits of data.  
A service updates those bits of data every so often.

This creates unnecessary network traffic and requires redis to handle billions of requests. 

WHAT IF:

When the service updates the bits of data: it triggers Redis Gears to add an event to a stream - notifying the Java services of the change to the data?

This eliminates unnecessary network traffic and requires redis to handle a few million events instead of billions of requests.

1. Code and register the Gear (Use RedisInsight to register simply). https://docs.redislabs.com/latest/ri/installing/install-docker/
2. Add logic to your Java Service layer to keep a copy of the bits of interesting data
3. Add logic to your Java Service layer to subscribe to an event channel to be notified of changes to the data
 


