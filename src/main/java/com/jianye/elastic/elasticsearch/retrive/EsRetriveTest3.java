/**
 * Project Name:elasticsearch-demo
 * File Name:EsRetriveTest2.java
 * Package Name:com.jianye.elastic.elasticsearch.retrive
 * Date:2017年6月30日下午3:17:45
 * Copyright (c) 2017, 963552657@qq.com All Rights Reserved.
 *
*/

package com.jianye.elastic.elasticsearch.retrive;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * ClassName:EsRetriveTest3 <br/>
 * Function: 主要应用Scroll查询验证. <br/>
 * Reason:	 TODO ADD REASON. <br/>
 * Date:     2017年6月30日 下午3:17:45 <br/>
 * @author   jianye
 * @version  
 * @since    JDK 1.6
 * @see 	 
 */
public class EsRetriveTest3 {
	
	public TransportClient client;
	
	@Before
	public void before() {
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("client.transport.sniff", true)
				.put("cluster.name", "swk_es")
				.put("client.transport.ping_timeout", 6000)
				.put("client.transport.nodes_sampler_interval", 6000)
				.build();
		client = new TransportClient(settings);
		client.addTransportAddress(new InetSocketTransportAddress("192.168.65.69", 9300));
		System.out.println("ElasticSearch open");
	}
	
//	@Test
	public void scrollTest() {
		
		// 滚动查询
		MatchAllQueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
		
		SortBuilder tsOrder = SortBuilders.fieldSort("ts").order(SortOrder.DESC);
		 
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch("yyy").setTypes("basic_data")
				.setScroll(TimeValue.timeValueMinutes(1))
				.setQuery(queryBuilder).addSort(tsOrder).setSize(10);
		// 打印请求的dsl语句
		System.out.println(searchRequestBuilder.toString());
	
		// 请求
		SearchResponse response = searchRequestBuilder.execute().actionGet();
		// 排序是正常的，第一次检索是有值的
		String scrollId = response.getScrollId();
		long totalHits = response.getHits().getTotalHits();
		System.out.println(scrollId + " " + totalHits);
		
		// 第一次请求的结果集
		SearchHit[] hits = response.getHits().getHits();
		System.out.println(hits.length);
		for (SearchHit hit : hits) {
			System.out.println(hit.getSourceAsString());
		}
		// scroll第n次请求有响应结果
		SearchScrollRequestBuilder scrollBuilder = client.prepareSearchScroll(scrollId)
				.setScroll(TimeValue.timeValueMinutes(1));
		System.out.println(scrollBuilder.toString());
		SearchResponse scrollResponse = scrollBuilder.execute().actionGet();
		// 更新scrollId
		scrollId = scrollResponse.getScrollId();
		System.out.println(scrollResponse.getHits().getHits().length);
	}
	
//	@Test
	public void scrollScanTest() {
		
		MatchAllQueryBuilder matchQuery = QueryBuilders.matchAllQuery();
		
		SearchRequestBuilder searchBuilder = client.prepareSearch("yyy").setTypes("basic_data")
			.setSearchType(SearchType.SCAN).setQuery(matchQuery)
			.setScroll(TimeValue.timeValueMinutes(1))
			.setSize(10);
		// 打印query dsl
		System.out.println(searchBuilder);
		// 响应结果
		SearchResponse response = searchBuilder.execute().actionGet();
		// 获取ScrollId
		String scrollId = response.getScrollId();
		long totalCount = response.getHits().getTotalHits();
		long firstCount = response.getHits().getHits().length;
		System.out.println( totalCount + " " + firstCount + " " + scrollId);
		
		// 单独再次进行遍历
		SearchScrollRequestBuilder scrollBuilder = client.prepareSearchScroll(scrollId)
				.setScroll(TimeValue.timeValueMinutes(1));
		System.out.println(scrollBuilder.toString());
		SearchResponse scrollResponse = scrollBuilder.execute().actionGet();
		SearchHit[] hits = scrollResponse.getHits().getHits();
		// 查询的数据量
		System.out.println(hits.length);
		for (SearchHit hit : hits) {
			System.out.println(hit.getSourceAsString());
		}
		// 二次更新scrollId
		scrollId = scrollResponse.getScrollId();
	}
	
	@Test
	public void repeatScrollIdTest() {
		
		MatchAllQueryBuilder matchBuilder = QueryBuilders.matchAllQuery();
		// 执行查询
		SearchRequestBuilder searchBuilder = client.prepareSearch("yyy").setTypes("basic_data")
				.setQuery(matchBuilder).setSearchType(SearchType.SCAN)
				.setScroll(TimeValue.timeValueMillis(1))
				.setSize(1);
		System.out.println(searchBuilder.toString());
		// 响应结果
		SearchResponse response = searchBuilder.execute().actionGet();
		// 响应结果
		String scrollId = response.getScrollId();
		long totalCount = response.getHits().getTotalHits();
		long firstCount = response.getHits().getHits().length;
		System.out.println(totalCount + " " + firstCount + " " + scrollId);
		
		// secondSearch
		SearchResponse scrollResponse = client.prepareSearchScroll(scrollId)
				.setScroll(TimeValue.timeValueMinutes(1)).execute()
				.actionGet();
		String newScrollId = scrollResponse.getScrollId();
		long secondCount = scrollResponse.getHits().getHits().length;
		System.out.println(secondCount + " " + newScrollId);
		SearchHit[] hits = scrollResponse.getHits().getHits();
		for (SearchHit hit : hits) {
			System.out.println(hit.getSourceAsString());
		}
		System.out.println("--------------------------------------我是可爱的分割线---------------------------");
		// thirdSearch 
		scrollResponse = client.prepareSearchScroll(scrollId)
			.setScroll(TimeValue.timeValueMinutes(1))
			.execute().actionGet();
		String thirdScrollId = scrollResponse.getScrollId();
		long thirdCount = scrollResponse.getHits().getHits().length;
		System.out.println(thirdCount + " " + thirdScrollId);
		hits = scrollResponse.getHits().getHits();
		for (SearchHit hit : hits) {
			System.out.println(hit.getSourceAsString());
		}
		// 验证结果，就算是同一个scrollId
	}
	
	@After
	public void after() {
		
		if (null != client) {
			System.out.println("ElasticSearch close");
			client.close();
		}
	}
}

