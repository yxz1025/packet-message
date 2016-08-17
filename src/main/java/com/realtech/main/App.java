package com.realtech.main;

import javax.rmi.CORBA.Util;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.realtech.consumer.MemberMessageConsumer;
import com.realtech.consumer.RedPacketsLogConsumer;
import com.realtech.util.ElasticUtil;
import com.realtech.util.Jredis;

public class App {

	public static void main(String[] args) {
		ApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
		MemberMessageConsumer consumer = (MemberMessageConsumer) context.getBean("memberMessageConsumer");
		consumer.execute();
//		RedPacketsLogConsumer consumer = (RedPacketsLogConsumer) context.getBean("redPacketsLogConsumer");
//		consumer.execute();
//		System.out.println("thread1 begin...");
	}

}
