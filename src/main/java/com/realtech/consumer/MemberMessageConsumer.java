package com.realtech.consumer;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.aliyun.mns.client.CloudAccount;
import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.common.http.ClientConfiguration;
import com.aliyun.mns.model.Message;
import com.fasterxml.jackson.dataformat.yaml.snakeyaml.events.Event;
import com.google.gson.JsonObject;
import com.qiniu.http.Response;
import com.qiniu.processing.OperationManager;
import com.qiniu.storage.UploadManager;
import com.qiniu.util.Auth;
import com.qiniu.util.StringMap;
import com.qiniu.util.UrlSafeBase64;
import com.realtech.util.ElasticUtil;
import com.realtech.util.HttpUtil;
import com.realtech.util.Jredis;
import com.realtech.util.PropsUtil;

/**
 * 
 * @author yuanxiaozhong 公众号消息事件消费
 */
public class MemberMessageConsumer {

	private ElasticUtil elasticUtil;

	// 阿里云key
	private String accesskey;
	private String secretKey;
	private int threadNum;

	// 消费队列名称
	private String queneRef;

	// 访问域
	private String MNSEndpoint;
	private CloudAccount cloudAccount;
	private MNSClient client;
	private CloudQueue queue;

	// redis
	private Jredis redis;

	// 并发度
	private ClientConfiguration clientConfiguration;

	// 七牛
	private Auth auth;
	private String ACCESS_KEY = "Q_EXBiZJnxJZwaKVxQzD0IcakVA49oAONkmVsTH0";
	private String SECRET_KEY = "bzumZIZkLwgy4dLUwKwbgyzZAlFJy3E4TnXnE7Nh";
	private String bucketname = "wxmedia";
	private UploadManager uploadManager;

	public MemberMessageConsumer(PropsUtil props) {
		this.accesskey = props.getValueByKey("mns.accesskey");
		this.secretKey = props.getValueByKey("mns.secretKey");
		this.queneRef = props.getValueByKey("mns.queneRef");
		this.MNSEndpoint = props.getValueByKey("mns.MNSEndpoint");
		this.threadNum = Integer.valueOf(props.getValueByKey("mns.threadNum"));
		this.clientConfiguration = new ClientConfiguration();
		this.clientConfiguration.setMaxConnections(this.threadNum);
		this.clientConfiguration.setMaxConnectionsPerRoute(this.threadNum);
		// 初始化队列
		cloudAccount = new CloudAccount(accesskey, secretKey, MNSEndpoint,
				clientConfiguration);
		client = cloudAccount.getMNSClient();

		// 初始化七牛
		this.auth = Auth.create(ACCESS_KEY, SECRET_KEY);
		this.uploadManager = new UploadManager();
	}

	public void setElasticUtil(ElasticUtil elasticUtil) {
		this.elasticUtil = elasticUtil;
	}

	public void setRedis(Jredis redis) {
		this.redis = redis;
	}

	/**
	 * 用户关注以后获取用户的基本信息
	 * 
	 * @throws Exception
	 * 
	 * @throws JSONException
	 */
	private void getUserInfo(JSONObject json) throws Exception {
		// 获取对应公众号的access_token
		String openid = json.getString("fromUserName");
		String appId = json.getString("appId");
		this.redis.getRedis().select(13);
		String content = this.redis.getRedis().hget(
				"authorization_access_token", appId);
		if (content != null) {
			JSONObject object = new JSONObject(content);
			String access_token = object.getString("authorizer_access_token");
			if (access_token != null) {
				String url = "https://api.weixin.qq.com/cgi-bin/user/info?access_token="
						+ access_token + "&openid=" + openid + "&lang=zh_CN";
				InputStream inputStream = HttpUtil.get(url);
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(inputStream));
				StringBuilder sb = new StringBuilder();
				String line = "";
				while ((line = reader.readLine()) != null) {
					sb.append(line + "\n");
				}
				JSONObject info = new JSONObject(sb.toString());
				if (!info.has("errcode")) {
					info.put("appId", json.getString("appId"));
					elasticUtil.createIndex("member_user", "user", openid,
							info.toString());
				}
			}
		}
	}

	/**
	 * 搜索公众号下某一个用户关注事件
	 * 
	 * @throws JSONException
	 */
	private void updateSubscribe(JSONObject json) throws JSONException {
		QueryBuilder builder = QueryBuilders
				.boolQuery()
				.must(QueryBuilders.termQuery("appId", json.get("appId")))
				.must(QueryBuilders.termQuery("fromUserName",
						json.get("fromUserName")))
				.must(QueryBuilders.termQuery("event", "subscribe"));
		SearchHit[] hits = elasticUtil.search(builder, "member_msg", "message");
		if (hits.length > 0) {
			SearchHit source = hits[0];
			String id = source.getId();
			String openid = json.getString("fromUserName");
			JSONObject doc = new JSONObject();
			doc.put("packetsId", json.getInt("packetsId"));
			elasticUtil.update(doc.toString(), "member_msg", "message", id);

			// 更新用户信息关联packetsId
			elasticUtil.update(doc.toString(), "member_user", "user", openid);
		}
	}

	/**
	 * 取消关注事件 1、判断用户关注事件是否有关联Id
	 * 
	 * @throws JSONException
	 */
	private JSONObject updateUnsubscribe(JSONObject json) throws JSONException {
		QueryBuilder builder = QueryBuilders
				.boolQuery()
				.must(QueryBuilders.termQuery("appId", json.get("appId")))
				.must(QueryBuilders.termQuery("fromUserName",
						json.get("fromUserName")))
				.must(QueryBuilders.termQuery("msgType", "event"))
				.must(QueryBuilders.termQuery("event", "subscribe"));
		SearchHit[] hits = elasticUtil.search(builder, "member_msg", "message");
		// 关联关键词
		if (hits.length > 0) {
			SearchHit source = hits[0];
			boolean flag = source.getSource().containsKey("packetsId");
			if (flag) {
				int packetsId = (Integer) source.getSource().get("packetsId");
				json.put("packetsId", packetsId);
			}
		}
		return json;
	}

	/**
	 * 获取微信media
	 * 
	 * @param appId
	 * @param mediaId
	 * @throws JSONException
	 */
	private byte[] getMediaData(String appId, String mediaId) throws Exception {
		this.redis.getRedis().select(13);
		String content = this.redis.getRedis().hget(
				"authorization_access_token", appId);
		if (content != null) {
			JSONObject object = new JSONObject(content);
			String access_token = object.getString("authorizer_access_token");
			String url = "https://api.weixin.qq.com/cgi-bin/media/get?access_token="
					+ access_token + "&media_id=" + mediaId;
			InputStream inputStream = HttpUtil.get(url);
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			byte[] buffer = new byte[1024];
			int len = 0;
			while ((len = inputStream.read(buffer)) != -1) {
				outputStream.write(buffer, 0, len);
			}
			inputStream.close();
			return outputStream.toByteArray();
		}
		return null;
	}

	/**
	 * 图片文件存储
	 * 
	 * @param message
	 * @throws Exception
	 */
	private JSONObject saveImageMedia(JSONObject obj) throws Exception {
		// 是否还有media_id
		if (!obj.has("mediaId")) {
			return obj;
		}
		String mediaId = obj.getString("mediaId");
		String appId = obj.getString("appId");
		long ctime = obj.getLong("createTime");
		byte[] data = this.getMediaData(appId, mediaId);
		if (data != null) {
			String key = appId + "/" + ctime + ".jpg";
			String upToken = auth.uploadToken(bucketname);
			Response res = this.uploadManager.put(data, key, upToken);
			if (res.isOK()) {
				obj.put("mediaId", key);
			}
		}
		return obj;
	}

	/**
	 * 多媒体文件存储
	 * 
	 * @throws Exception
	 */
	private JSONObject saveVoiceMedia(JSONObject obj) throws Exception {
		// 是否还有media_id
		if (!obj.has("mediaId")) {
			return obj;
		}
		String mediaId = obj.getString("mediaId");
		String appId = obj.getString("appId");
		long ctime = obj.getLong("createTime");
		byte[] data = this.getMediaData(appId, mediaId);
		if (data != null) {
			String key = appId + "/" + ctime + ".mp3";
			String fops = "avthumb/mp3/ab/128k/ar/44100/acodec/libmp3lame";
			// 设置转码的队列
			String pipeline = "wxpop";
			// 可以对转码后的文件使用saveas参数自定义命名，当然也可以不指定，文件会默认命名并保存在当前空间
			String urlbase64 = UrlSafeBase64.encodeToString(bucketname + ":"
					+ key);
			String pfops = fops + "|saveas/" + urlbase64;
			String upToken = auth.uploadToken(bucketname, null, 3600,
					new StringMap().putNotEmpty("persistentOps", pfops)
							.putNotEmpty("persistentPipeline", pipeline), true);
			// 调用put方法上传
			Response res = uploadManager.put(data, key, upToken);
			// 打印返回的信息
			if (res.isOK()) {
				obj.put("mediaId", key);
			}
		}
		return obj;
	}
	
	/**
	 * 保存微信视频
	 * @param message
	 * @throws Exception 
	 */
	private JSONObject saveVideoMedia(JSONObject obj) throws Exception{
		// 是否还有media_id
		if (!obj.has("mediaId")) {
			return obj;
		}
		String mediaId = obj.getString("mediaId");
		String appId = obj.getString("appId");
		long ctime = obj.getLong("createTime");
		byte[] data = this.getMediaData(appId, mediaId);
		if (data != null) {
			String key = appId + "/" + ctime + ".mp4";
			String upToken = auth.uploadToken(bucketname);
			Response res = this.uploadManager.put(data, key, upToken);
			if (res.isOK()) {
				obj.put("mediaId", key);
			}
		}
		return obj;
	}

	/**
	 * 消息分发
	 */
	private void apartMessage(String msg) {
		JSONObject json;
		try {
			json = new JSONObject(msg);
			String msgType = json.getString("msgType");
			//处理特殊的媒体消息保存七牛
			if (msgType.equals("shortvideo") || msgType.equals("video")) {
				json = saveVideoMedia(json);
			}
			
			if (msgType.equals("image")) {
				json = saveImageMedia(json);
			}
			
			if (msgType.equals("voice")) {
				json = saveVoiceMedia(json);
			}
			
			//查询用户信息是否存在
			String openid = json.getString("fromUserName");
			GetResponse response = this.elasticUtil.getIndex("member_user", "user", openid);
			if (!response.isExists()) {
				getUserInfo(json);
			}
			
			if (msgType.equals("text")) {
				// 判断关键词是否匹配
				if (json.has("keywords")) {
					updateSubscribe(json);
				}
				elasticUtil.createIndex("member_msg", "message",
						json.toString());

			} else {
				if (msgType.equals("event")) {
					String event = json.getString("event");
					if (event.equals("unsubscribe")) {
						// 取消关注事件
						json = updateUnsubscribe(json);
					}
				}
				elasticUtil.createIndex("member_msg", "message",
						json.toString());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 消息事件 1、判断是哪种消息类型 2、关注事件和取消关注事件为一个公众号只有一种状态，此处需要标记关注渠道，（关键词红包来源） 3、普通消息
	 */
	public void execute() {

		for (int i = 0; i < threadNum; i++) {
			Thread thread = new Thread(new Runnable() {

				public void run() {
					queue = client.getQueueRef(queneRef);
					while (true) {
						try {
							Message popMsg = queue.popMessage();
							if (popMsg != null) {

								// 普通消息 直接索引，事件消息 判断是否为关注或取消关注事件 每个公众号用户关注只有两个状态
								String obj = popMsg.getMessageBodyAsString();
								apartMessage(obj);
								// 删除已经消费的消息
								queue.deleteMessage(popMsg.getReceiptHandle());
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
