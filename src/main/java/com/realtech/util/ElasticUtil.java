package com.realtech.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

public class ElasticUtil {

	private Client client;
	private BulkRequestBuilder bulkRequest;

	public ElasticUtil(PropsUtil props) {
		try {
			String ipAddress = props.getValueByKey("elastic.ipAddress");
			String cluster = props.getValueByKey("elastic.cluster");

			Settings settings = Settings.settingsBuilder()
					.put("cluster.name", cluster).build();
			client = TransportClient
					.builder()
					.settings(settings)
					.build()
					.addTransportAddress(
							new InetSocketTransportAddress(InetAddress
									.getByName(ipAddress), 9300));
			bulkRequest = client.prepareBulk();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// 批量创建索引
	public BulkResponse createIndexBulk() {
		BulkResponse bulkResponse = bulkRequest.get();
		return bulkResponse;
	}

	// 设置bulk
	public void setBulk(String index, String type, String jsonData) {
		bulkRequest.add(client.prepareIndex(index, type)
				.setSource(jsonData));
	}

	public void setBulk(String index, String type, String id, String jsonData) {
		bulkRequest.add(client.prepareIndex(index, type, id)
				.setSource(jsonData));
	}
	
	// 建立单条索引
	public IndexResponse createIndex(String index, String type, String id,
			String jsonData) {
		IndexResponse response = null;
		response = client.prepareIndex(index, type, id).setSource(jsonData)
				.get();
		return response;
	}
	
	public IndexResponse createIndex(String index, String type,
			String jsonData) {
		IndexResponse response = null;
		response = client.prepareIndex(index, type).setSource(jsonData)
				.get();
		return response;
	}
	
	// 获取单条记录
	public GetResponse getIndex(String index, String type, String id) {
		GetResponse response = client.prepareGet(index, type, id).get();
		return response;
	}
	
	// 删除一条记录
	public DeleteResponse deleteIndex(String index, String type, String id) {
		DeleteResponse response = client.prepareDelete(index, type, id).get();
		return response;
	}

	// 获取数据
	public SearchHit[] search(QueryBuilder builder, String index, String type){
		SearchResponse response = client.prepareSearch(index)
										.setTypes(type)
										.setQuery(builder)
										.execute()
										.actionGet();
		SearchHits hits = response.getHits();
		SearchHit[] searchHists = hits.getHits();
		return searchHists;
	}
	
	//更新索引
	public UpdateResponse update(String doc, String index, String type, String id){
		UpdateResponse response = client.prepareUpdate(index, type, id)
										.setDoc(doc)
										.get();
		return response;
	}
	
	// 关闭链接
	public void close() {
		client.close();
	}
}
