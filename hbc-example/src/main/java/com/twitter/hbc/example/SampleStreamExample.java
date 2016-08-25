/**
 * Copyright 2013 Twitter, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.twitter.hbc.example;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class SampleStreamExample {

  public static void run(String consumerKey, String consumerSecret, String token, String secret) throws InterruptedException {
    // Create an appropriately sized blocking queue
    BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10);

    // Define our endpoint: By default, delimited=length is set (we need this for our processor)
    // and stall warnings are on.
    StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
    Location.Coordinate southwest = new Location.Coordinate(-180, -90);
    Location.Coordinate northeast = new Location.Coordinate(180, 90);
    Location region = new Location(southwest, northeast);
    endpoint.locations(Lists.newArrayList(region));

    Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
    //Authentication auth = new com.twitter.hbc.httpclient.auth.BasicAuth(username, password);

    // Create a new BasicClient. By default gzip is enabled.
    BasicClient client = new ClientBuilder()
            .name("sampleExampleClient")
            .hosts(Constants.STREAM_HOST)
            .endpoint(endpoint)
            .authentication(auth)
            .processor(new StringDelimitedProcessor(queue))
            .build();

    // Establish a connection
    client.connect();
    JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");

    // Do whatever needs to be done with messages
    while (true) {
      if (client.isDone()) {
        System.out.println("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
        break;
      }

      String msg = queue.poll(5, TimeUnit.SECONDS);
      if (msg == null) {
        System.out.println("Did not receive a message in 5 seconds");
      } else {
        try {
          ObjectMapper mapper = new ObjectMapper();
          JsonFactory factory = mapper.getFactory();
          JsonParser jp = factory.createParser(msg);
          JsonNode jsonNode = mapper.readTree(jp);
          JsonNode user = jsonNode.get("user");
          JsonNode place = jsonNode.get("place");
          if (place != null && !place.isNull()) {

            List<String> words = Arrays.asList(jsonNode.get("text").toString().replace("#","").replace("@","").split(" "));
            Double lat = place.get("bounding_box").get("coordinates").get(0).get(0).get(0).asDouble();
            Double longitude = place.get("bounding_box").get("coordinates").get(0).get(0).get(1).asDouble();
            ObjectNode jNode = mapper.createObjectNode();
            ArrayNode coord = jNode.putArray("coordinates");
            coord.add(lat);
            coord.add(longitude);
            JsonNode entities = jsonNode.get("entities");
            if (entities != null && !entities.isNull()) {
              JsonNode hashtagsJsonNode = entities.get("hashtags");
              JsonNode userMentionsJsonNode = entities.get("user_mentions");
              if ((hashtagsJsonNode != null && !hashtagsJsonNode.isNull() && hashtagsJsonNode.size() > 0) || (userMentionsJsonNode != null && !userMentionsJsonNode.isNull() && userMentionsJsonNode.size() > 0)) {
                List<String> keys = new ArrayList<String>();
                for (int i = 0; i < hashtagsJsonNode.size(); i++) {
                  keys.add(hashtagsJsonNode.get(i).get("text").asText().toLowerCase());
                }
                for (int i = 0; i < userMentionsJsonNode.size(); i++) {
                  keys.add(userMentionsJsonNode.get(i).get("screen_name").asText().toLowerCase());
                }

                ObjectNode userNode = mapper.createObjectNode();
                ObjectNode tweet  = jNode.putObject("tweet");
                tweet.put("user","@" + user.get("screen_name").asText());
                tweet.put("img",user.get("profile_image_url").asText());
                tweet.put("text",jsonNode.get("text").asText());
                System.out.println(jNode.toString());
                sendToRedis(pool, keys,jNode);
              }
            }


          }
        } catch (Exception e) {
          e.printStackTrace();
        }

      }
    }
    pool.destroy();
    client.stop();

    // Print some stats
    System.out.printf("The client read %d messages!\n", client.getStatsTracker().getNumMessages());
  }

  private static void sendToRedis(JedisPool pool, List<String> keys, JsonNode tweetData) {
    Jedis jedis = null;
    try {
      jedis = pool.getResource();
      if(jedis.llen("user:test")>200){
        jedis.rpop("user:test");
      }
      jedis.lpush("user:test",tweetData.toString());
      for(String key:keys){
        Set<String> members = jedis.smembers(key.toLowerCase());
        String currentLeaderBoardKey = buildLeaderBoardKey(0);
        String nextLeaderBoardKey = buildLeaderBoardKey(1);
        jedis.zincrby(currentLeaderBoardKey,1D,key);
        jedis.zincrby(nextLeaderBoardKey,1D,key);

        if(jedis.ttl(currentLeaderBoardKey)<0){
          jedis.expire(currentLeaderBoardKey,3600);
        }

        if(jedis.ttl(nextLeaderBoardKey)<0){
          jedis.expire(nextLeaderBoardKey,7200);
        }

        if(!members.isEmpty()){
          for (String member:members){
            String userKey = format("user:%s",member);
            if(jedis.llen(userKey)>20){
              jedis.rpop(userKey);
            }
            jedis.lpush(userKey,tweetData.toString());
          }
        }

      }
    } finally {
      if (jedis != null) {
        jedis.close();
      }
    }
  }

  private static String buildLeaderBoardKey(int incr) {
    int hour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY) + incr;
    return format("leaderbard:scores:%s",hour);
  }


  public static void main(String[] args) {
    try {
      String consumerKey = "XXX";
      String consumerSecret = "YYY";
      String token = "ZZZ";
      String secret = "LLLL";
      if(consumerKey.equals("XXX")){
        System.out.println("You should Twitter keys");
      }
      SampleStreamExample.run(consumerKey, consumerSecret, token, secret);
    } catch (InterruptedException e) {
      System.out.println(e);
    }
  }
}
