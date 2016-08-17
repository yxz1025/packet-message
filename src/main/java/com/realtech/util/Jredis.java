package com.realtech.util;


import redis.clients.jedis.Jedis;

public class Jredis {

	private Jedis redis;
	
	public Jredis(PropsUtil props) {
		try {
			String host = props.getValueByKey("redis.host");
			String auth = props.getValueByKey("redis.auth");
			int port = 6379;
			this.redis = new Jedis(host, port);
			this.redis.auth(auth);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Jedis getRedis() {
		return redis;
	}

}
