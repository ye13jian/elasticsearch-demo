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
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryFilterBuilder;
import org.elasticsearch.index.query.TermFilterBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * ClassName:EsRetriveTest2 <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason:	 TODO ADD REASON. <br/>
 * Date:     2017年6月30日 下午3:17:45 <br/>
 * @author   admin
 * @version  
 * @since    JDK 1.6
 * @see 	 
 */
public class EsRetriveTest2 {
	
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
	public void fieldTest() {
		
		TermQueryBuilder queryBuilders =  QueryBuilders.termQuery("person_id", "04-56-04-6E-29-34");
		SearchResponse response = client.prepareSearch("swk").setTypes("sample_person")
			.setQuery(queryBuilders).execute().actionGet();
		long totalHits = response.getHits().getTotalHits();
		System.out.println(totalHits);
	}
	
//	@Test
	public void searchMac() {
		
		TermQueryBuilder queryBuilder = QueryBuilders.termQuery("mac", "04-56-04-6E-29-34");
//		TermQueryBuilder queryBuilder = QueryBuilders.termQuery("sid", "iat9733a57f@gz02120c61e62f3c8e00");
		System.out.println(queryBuilder.toString());
		SearchResponse response = client.prepareSearch("yyy").setTypes("basic_data")
				.setQuery(queryBuilder).execute().actionGet();
		long totalHits = response.getHits().getTotalHits();
//		System.out.println(response.getHits().get);
		System.out.println(totalHits);
	}
	
//	@Test
	public void searchMac2() {
		
		TermFilterBuilder filterBuilder = FilterBuilders.termFilter("mac", "04-56-04-6E-29-34");
		System.out.println(filterBuilder.toString());
		SearchResponse response = client.prepareSearch("yyy").setTypes("basic_data")
			.setPostFilter(filterBuilder).execute().actionGet();
		long totalHits = response.getHits().totalHits();
		System.out.println(totalHits);
	}
	
//	@Test
	public void searchFiltered () {
		
		MatchQueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery("person_id", "04-56-04-6E-29-34");
		QueryFilterBuilder filterBuilder  = FilterBuilders.queryFilter(queryBuilder);
		FilteredQueryBuilder queryFiltered = new FilteredQueryBuilder(null, filterBuilder);
		System.out.println(queryFiltered.toString());
		SearchResponse response = client.prepareSearch("swk").setTypes("sample_person")
			.setPostFilter(filterBuilder).execute().actionGet();
		long totalHits = response.getHits().getTotalHits();
		System.out.println(totalHits);
	}
	
	@Test
	public void queryFilter() {
		
		TermQueryBuilder queryBuilder = QueryBuilders.termQuery("person_id", "04-56-04-6E-29-34");
		TermFilterBuilder filterBuilder =  FilterBuilders.termFilter("person_id", "04-56-04-6E-29-34");
		
		SearchRequestBuilder builder = client.prepareSearch("swk").setTypes("sample_person")
			.setQuery(queryBuilder).setPostFilter(filterBuilder);
		System.out.println(builder.toString());
		SearchResponse response = builder.execute().actionGet();
		long totalHits = response.getHits().getTotalHits();
		System.out.println(totalHits);
	}
	
	@After
	public void after() {
		
		if (null != client) {
			System.out.println("ElasticSearch close");
			client.close();
		}
	}
}

