package com.ideal.flume.cache;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

public class RedisClientUtils {
	private static Map<String, JedisCluster> jedisClusterMap = new HashMap<String, JedisCluster>();
	private static Map<String, JedisPool> jedisPoolMap = new HashMap<String, JedisPool>();

	public static synchronized JedisCommands getJedisCmd(String redisUrl) {
		if (jedisClusterMap.containsKey(redisUrl))
			return jedisClusterMap.get(redisUrl);

		if (jedisPoolMap.containsKey(redisUrl))
			return jedisPoolMap.get(redisUrl).getResource();

		String[] arr = redisUrl.split(",");
		if (arr.length == 0) {
			throw new IllegalArgumentException("unknown redis config.");
		}

		Set<HostAndPort> nodes = new HashSet<HostAndPort>(arr.length);
		for (String s : arr) {
			int i = s.indexOf(":");
			nodes.add(new HostAndPort(s.substring(0, i), Integer.parseInt(s.substring(i + 1))));
		}

		if (nodes.size() == 1) {
			HostAndPort hap = nodes.iterator().next();
			JedisPool jedisPool = new JedisPool(hap.getHost(), hap.getPort());
			jedisPoolMap.put(redisUrl, jedisPool);
			return jedisPool.getResource();
		} else {
			JedisCluster jedisCluster = new JedisCluster(nodes);
			jedisClusterMap.put(redisUrl, jedisCluster);
			return jedisCluster;
		}
	}

	public static synchronized JedisCommands getJedisCmd(String redisUrl, String password) {
		if (jedisClusterMap.containsKey(redisUrl))
			return jedisClusterMap.get(redisUrl);

		if (jedisPoolMap.containsKey(redisUrl))
			return jedisPoolMap.get(redisUrl).getResource();

		String[] arr = redisUrl.split(",");
		if (arr.length == 0) {
			throw new IllegalArgumentException("unknown redis config.");
		}

		Set<HostAndPort> nodes = new HashSet<HostAndPort>(arr.length);
		for (String s : arr) {
			int i = s.indexOf(":");
			nodes.add(new HostAndPort(s.substring(0, i), Integer.parseInt(s.substring(i + 1))));
		}
		
		//添加redis配置测试最大连接数。2018-0628 seamus
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxWaitMillis(10 * 1000);
		config.setMaxIdle(20);
        config.setMaxTotal(40);
        config.setMinIdle(10);
		config.setTestOnBorrow(true);

		if (nodes.size() == 1) {
			HostAndPort hap = nodes.iterator().next();
			JedisPool jedisPool = new JedisPool(config, hap.getHost(), hap.getPort(),
					Protocol.DEFAULT_TIMEOUT, password);
			jedisPoolMap.put(redisUrl, jedisPool);
			return jedisPool.getResource();
		} else {
			JedisCluster jedisCluster = new JedisCluster(nodes, 2000, 2000, 5, password,config);
			jedisClusterMap.put(redisUrl, jedisCluster);
			return jedisCluster;
		}

		//原版本程序。添加redis配置测试最大连接数。2018-0628 seamus
		/*if (nodes.size() == 1) {
			HostAndPort hap = nodes.iterator().next();
			JedisPool jedisPool = new JedisPool(new GenericObjectPoolConfig(), hap.getHost(), hap.getPort(),
					Protocol.DEFAULT_TIMEOUT, password);
			jedisPoolMap.put(redisUrl, jedisPool);
			return jedisPool.getResource();
		} else {
			JedisCluster jedisCluster = new JedisCluster(nodes, 2000, 2000, 5, password, new GenericObjectPoolConfig());
			jedisClusterMap.put(redisUrl, jedisCluster);
			return jedisCluster;
		}*/
	}
}
