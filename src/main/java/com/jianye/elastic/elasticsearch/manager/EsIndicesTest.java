/**
 * Project Name:elasticsearch-demo
 * File Name:EsManagerTest.java
 * Package Name:com.jianye.elastic.elasticsearch.manager
 * Date:2017年6月30日上午9:28:26
 * Copyright (c) 2017, 963552657@qq.com All Rights Reserved.
 *
*/

package com.jianye.elastic.elasticsearch.manager;

import java.io.IOException;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateResponse;
import org.elasticsearch.action.admin.indices.warmer.delete.DeleteWarmerResponse;
import org.elasticsearch.action.admin.indices.warmer.put.PutWarmerResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.facet.FacetBuilders;
import org.junit.After;
import org.junit.Before;

/**
 * ClassName:EsManagerTest <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason:	 TODO ADD REASON. <br/>
 * Date:     2017年6月30日 上午9:28:26 <br/>
 * @author   admin
 * @version  
 * @since    JDK 1.6
 * @see 	 
 */
public class EsIndicesTest {
	
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
	
	/** index exist */
	public void indexExist() {
		
		IndicesExistsResponse response = client.admin().indices()
				.prepareExists("books", "library")
				.execute().actionGet();
		
		response.isExists();
	}
	
	/** type exist */
	public void typeExist() {
		
		TypesExistsResponse response = client.admin().indices()
				.prepareTypesExists("library")
				.setTypes("book")
				.execute().actionGet();
		
		response.isExists();
	}
	
	/** index statistics */
	public void indexStatistics() {
		
		IndicesStatsResponse response = client.admin().indices()
				.prepareStats("library")
				.all()
				.execute().actionGet();
		
		response.getSuccessfulShards();
	}
	
	/** index state */
	public void indexState() {
		
		IndicesStatsResponse response = client.admin().indices()
				.prepareStats("library")
				.setRecovery(true)
				.execute().actionGet();
		
		response.getSuccessfulShards();
	}
	
	/** index segment */
	public void indexSegments() {
		
		IndicesSegmentResponse response = client.admin().indices()
				.prepareSegments("library")
				.execute().actionGet();
		
		response.getSuccessfulShards();
	}
	
	public void indexCreate() {
		
		try {
			CreateIndexResponse response = client.admin().indices()
					.prepareCreate("nes")
					.setSettings(ImmutableSettings.settingsBuilder()
							.put("number_of_shards", 1))
					.addMapping("new", XContentFactory.jsonBuilder()
							.startObject()
								.startObject("news")
								 .startObject("properties")
								  .startObject("title")
								  	.field("analyzer", "whitespace")
								  	.field("type", "String")
								  .endObject()
								 .endObject()
								.endObject()
								.endObject())
					.execute().actionGet();
			
			response.getHeaders();
		} catch (ElasticsearchException e) {
			
			e.printStackTrace();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
				
	}
	
	/** index delete */
	public void  indexDelete() {
		
		DeleteIndexResponse response = client.admin().indices()
				.prepareDelete("news")
				.execute().actionGet();
		
		response.isAcknowledged();
	}
	
	/** index close */
	public void indexClose() {
		
		CloseIndexResponse response = client.admin().indices()
				.prepareClose("library")
				.execute().actionGet();
		
		response.isAcknowledged();
	}
	
	/** index open */
	public void indexOpen() {
		
		OpenIndexResponse response = client.admin().indices()
				.prepareOpen("library")
				.execute().actionGet();
		
		response.isAcknowledged();
	}
	
	/** index refresh */
	public void indexRefresh() {
		
		RefreshResponse response = client.admin().indices()
				.prepareRefresh("library")
				.execute().actionGet();
		
		response.getTotalShards();
	}
	
	/** clean buffer */
	public void cleanBuffer() {
		
		FlushResponse response = client.admin().indices()
				.prepareFlush("library")
				.setForce(true)
				.execute().actionGet();
		
		response.getFailedShards();
	}
	
	/** index optimize */
	public void indexopotimize() {
		
		OptimizeResponse response = client.admin().indices()
				.prepareOptimize("library")
				.setMaxNumSegments(2)
				.setFlush(true)
				.setOnlyExpungeDeletes(false)
				.execute().actionGet();
		
		response.getHeaders();
	}
	
	/** put mapping */
	public void setMapper() {
		
		try {
			PutMappingResponse response = client.admin().indices()
					.preparePutMapping("news")
					.setType("news")
					.setSource(XContentFactory.jsonBuilder()
							.startObject()
								.startObject("news")
									.startObject("properties")
										.startObject("title")
											.field("annalyzer", "whitespace")
											.field("type", "string")
										.endObject()
									.endObject()
								.endObject()
							.endObject())
					.execute().actionGet();
			
			response.getHeaders();
		} catch (ElasticsearchException e) {
			
			e.printStackTrace();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
	}
	
	/** delete mapping */
	public void deletemapping() {
		
		DeleteMappingResponse response = client.admin().indices()
				.prepareDeleteMapping("news")
				.setType("news")
				.execute().actionGet();
		
		response.getHeaders();
	}
	
	/** get snapshot */
	public void getsnapshot() {
		
		// API 估计出现修改
	}
	
	/** aliases settings */
	public void aliase() {
		
		/*IndicesAliasesResponse response = client.admin().indices()
				.prepareGetAliases()
				.addAliases("news", "n").("library", "elastic_books", 
						FilterBuilders.termFilter("title", "elasticSearch"))
				
				.execute().actionGet();
		
		response.getHeaders();*/
	}
	
	/** get aliase */
	public void getAliase() {
		
		GetAliasesResponse response = client.admin().indices()
				.prepareGetAliases("elastic_books", "n")
				.execute().actionGet();
		
		response.getHeaders();
	}
	
	/** aliase exists */
	public void aliaseexists() {
		
		AliasesExistResponse response = client.admin().indices()
				.prepareAliasesExist("elastic*", "unknown")
				.execute().actionGet();
		
		response.exists();
	}
	
	/** clear cache */
	public void clearCache() {
		
		ClearIndicesCacheResponse response = client.admin().indices()
				.prepareClearCache("library")
				.setFieldDataCache(true)
				.setFields("title")
				.setFilterCache(true)
				.setIdCache(true)
				.execute().actionGet();
		
		response.getTotalShards();
	}
	
	/** update setting */
	public void updatesetting() {
		
		UpdateSettingsResponse response = client.admin().indices()
				.prepareUpdateSettings("library")
				.setSettings(ImmutableSettings.builder()
						.put("index.number_of_replicas", 2))
				.execute().actionGet();
		
		response.getHeaders();
	}
	
	/** analyze */
	public void analyze() {
		
		AnalyzeResponse response = client.admin().indices()
				.prepareAnalyze("library", "ElasticSearch Server")
				.setTokenizer("whitespace")
				.setTokenFilters("nGram")
				.execute().actionGet();
		
		response.getTokens();
	}
	
	/** put template */
	public void putTemplate() {
		
//		PutIndexTemplateResponse response = client.admin().indices()
//				.preparePutMapping("my_template")
//				.set
	}
	
	/** delete template */
	public void deleteTemplate() {
		
		DeleteIndexTemplateResponse response = client.admin().indices()
				.prepareDeleteTemplate("my_*")
				.execute().actionGet();
		
		response.getHeaders();
	}
	
	/** validate query */
	public void validateQuery() {
		
		// TODO
//		ValidateQueryResponse response = client.admin().indices()
//				.prepareValidateQuery("library")
//				.setExplain(true)
//				.setQuery(XContentFactory.jsonBuilder()
//						.startObject()
//							.field("name").value("elastic search")
//						.endObject().bytes())
	}
	
	/** put warmer */
	@SuppressWarnings("deprecation")
	public void putWarmer() {
		
		PutWarmerResponse response = client.admin().indices()
				.preparePutWarmer("library_warmer")
				.setSearchRequest(client.prepareSearch("library")
						.addFacet(FacetBuilders.termsFacet("tags").field("tags")))
				.execute().actionGet();
		
		response.getHeaders();
	}
	
	/** delete warmer */
	public void deletewarmer() {
		
		DeleteWarmerResponse response = client.admin().indices()
				.prepareDeleteWarmer()
				.setNames("library_*")
				.execute().actionGet();
		response.getHeaders();
	}
	
	@After
	public void after() {
		
		if (null != client) {
			System.out.println("ElasticSearch close");
			client.close();
		}
	}
}

