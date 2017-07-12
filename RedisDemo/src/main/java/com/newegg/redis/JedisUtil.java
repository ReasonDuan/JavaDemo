package com.newegg.redis;

import com.reason.util.RandomUtil;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by rd87 on 5/24/2017.
 */
public class JedisUtil {

    private JedisCluster jedis;
    private Object keySpaces="";
    private Integer ttl = 0;
    public JedisUtil(String hosts, String keySpace, Integer ttl) {
        this.keySpaces = keySpace+":";
        this.ttl = ttl;
        final String[] hostports = hosts.split(",");
        Set<HostAndPort> nodes = new HashSet<HostAndPort>(){
            private static final long serialVersionUID = 5341345879054512402L; {
                for (String hostAndPort : hostports) {
                    String[] ipport = hostAndPort.split(":");
                    if(ipport.length > 1){
                        add(new HostAndPort(ipport[0], Integer.valueOf(ipport[1])));
                    }else{
                        add(new HostAndPort(ipport[0], 6379));
                    }
                }
            }
        };
        jedis = new JedisCluster(nodes);
    }


    public void hset(String key, String item, Set<String> sets){
        jedis.hset(keySpaces+key, item, sets.toString());
        jedis.expire(keySpaces+key,ttl);
    }

    public boolean exists(String key, String value){
        if(null != jedis.hget(keySpaces+key,value)){
            return true;
        }
        return false;
    }

    public void hgetAll(String key){
        Map<String, String> list = jedis.hgetAll(keySpaces+key);
        System.out.println(list.toString());
    }
    public Set<String> hget(String key, String item){
        String value = jedis.hget(keySpaces+key, item);
        System.out.println(value);
        return getSets(value);
    }

    private Set<String> getSets(String value){
        if(value == null || value.length() < 3){
            return null;
        }
        Set<String> wishs = new HashSet<String>();
        value = value.substring(1);
        value = value.substring(0,value.length()-1);
        String[] values = value.split(",");
        for(String wishlist : values){
            wishs.add(wishlist);
        }
        return wishs;
    }


    public static void main(String[] args) {
        JedisUtil jedisUtil = new JedisUtil("10.16.238.83:8000","REASON",600);
        Set<String> sets = new HashSet<String>();
        for(int i=0; i<50; i++){
            sets.add(""+ RandomUtil.getAge());
        }
        //sets.add("369");
        //jedisUtil.hset("WISHLISTITEMKEY","aa-bbb-ccc",sets);
        jedisUtil.hset("WISHLISTITEMKEY","aa-bbb-ccc",sets);
        //jedisUtil.hgetAll("WISHLISTITEMKEY");

        System.out.println(jedisUtil.hget("WISHLIST:ITEMWISHLISTCACHEKEY","32-102-262 "));


        long start = System.currentTimeMillis();
//        for(int j=0; j<10000; j++){
//            Set<String> wishlist = jedisUtil.hget("WISHLISTITEMKEY","aa-bbb-ccc");
//            //System.out.println(wishlist);
//        }
        System.out.println("Use time " + (System.currentTimeMillis()-start) + " ms.");
        System.out.println(jedisUtil.exists("WISHLISTITEMKEY","cc-ddd-aaa"));
    }


}
