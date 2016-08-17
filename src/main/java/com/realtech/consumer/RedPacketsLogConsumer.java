package com.realtech.consumer;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.action.index.IndexResponse;

import com.aliyun.mns.client.CloudAccount;
import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.common.http.ClientConfiguration;
import com.aliyun.mns.model.Message;
import com.realtech.util.ElasticUtil;
import com.realtech.util.Jredis;
import com.realtech.util.PropsUtil;

public class RedPacketsLogConsumer {
	
	private ElasticUtil elasticUtil;

	// 阿里云key
	private String accesskey;
	private String secretKey;
	private int threadNum;

	// 消费队列名称
	private String queneRef = "redpackets-send-quene";

	// 访问域
	private String MNSEndpoint;
	private CloudAccount cloudAccount;
	private MNSClient client;
	private CloudQueue queue;
	
	//redis
	private Jredis redis;

	// 并发度
	private ClientConfiguration clientConfiguration;
	
	public RedPacketsLogConsumer(PropsUtil props) {
		this.accesskey = props.getValueByKey("mns.accesskey");
		this.secretKey = props.getValueByKey("mns.secretKey");
		this.MNSEndpoint = props.getValueByKey("mns.MNSEndpoint");
		this.threadNum = Integer.valueOf(props.getValueByKey("mns.threadNum"));
		this.clientConfiguration = new ClientConfiguration();
		this.clientConfiguration.setMaxConnections(this.threadNum);
		this.clientConfiguration.setMaxConnectionsPerRoute(this.threadNum);
		// 初始化队列
		cloudAccount = new CloudAccount(accesskey, secretKey, MNSEndpoint,
				clientConfiguration);
		client = cloudAccount.getMNSClient();		
	}
	
	public void setElasticUtil(ElasticUtil elasticUtil) {
		this.elasticUtil = elasticUtil;
	}
	
	
	public void setRedis(Jredis redis) {
		this.redis = redis;
	}
	
	//记录抢红包记录
	private boolean redPacketLog(String msg){
		IndexResponse response = this.elasticUtil.createIndex("member_redpackets", "sendLog", msg);
		return response.isCreated();
	}
	
	//执行消息消费
	public void execute(){
		for (int i = 0; i < threadNum; i++) {
			Thread thread = new Thread(new Runnable() {

				public void run() {
					queue = client.getQueueRef(queneRef);
					while (true) {
						try {
							Message popMsg = queue.popMessage();
							if (popMsg != null) {
								
								//普通消息 直接索引，事件消息 判断是否为关注或取消关注事件 每个公众号用户关注只有两个状态
								String obj = popMsg.getMessageBodyAsString();
								System.out.println("success insert...." + obj);
								boolean flag = redPacketLog(obj);
								// 删除已经消费的消息
								if (flag) {
									queue.deleteMessage(popMsg.getReceiptHandle());	
								}
								System.out.println("success insert...." + obj);
								
							}
							Thread.currentThread().sleep(500);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}

			}, String.valueOf(i));
			thread.start();
		}		
	}

}
