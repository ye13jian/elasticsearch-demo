/**
 * Project Name:elasticsearch-demo
 * File Name:EsRetriveBasicTest.java
 * Package Name:com.jianye.elastic.elasticsearch.retrive
 * Date:2017年6月28日下午8:39:43
 * Copyright (c) 2017, 963552657@qq.com All Rights Reserved.
 *
*/

package com.jianye.elastic.elasticsearch.retrive;

import java.io.IOException;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.percolate.PercolateResponse.Match;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.facet.FacetBuilder;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * ClassName:EsRetriveBasicTest <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason:	   简单的查询 	<br/>
 * Date:     2017年6月28日 下午8:39:43 <br/>
 * @author   admin
 * @version  
 * @since    JDK 1.6
 * @see 	 
 */
public class EsRetriveBasicTest {
	
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
	
	@Test
	public void simpleRetrive() {
		
		// 读取文档
//		client.prepareGet()
		// 索引文档
//		client.prepareIndex()
		// 更新文档
//		client.prepareUpdate()
		// 删除文档
//		client.prepareDelete()
		// 准备查询
//		client.prepareSearch()
		// 查询构建器 QueryBuilder
//		client.prepareSearch("index").setQuery(queryBuilder)
		
	}
	
	/** dismax查询 */
	public void dismaxQuery() {
		// 词项查询 termQuery 前缀查询 prefixQuery
		QueryBuilder queryBuilder = QueryBuilders.disMaxQuery()
				.add(QueryBuilders.termQuery("title", "Elastic"))
				.add(QueryBuilders.prefixQuery("title", "el"));
		System.out.println(queryBuilder.toString());
		SearchResponse response = client.prepareSearch("library")
				.setQuery(queryBuilder).execute().actionGet();
		response.getHits();
	}
	
	/** page */
	public void page() {
		SearchResponse response = client.prepareSearch("library")
				.setQuery(QueryBuilders.matchAllQuery())
				.setFrom(10)
				.setSize(20)
				.execute().actionGet();
		response.getHits();
	}
	
	/** sort */
	public void sort() {
		SearchResponse response = client.prepareSearch("library")
				.setQuery(QueryBuilders.matchAllQuery())
				.addSort(SortBuilders.fieldSort("title"))
				.addSort("_score", SortOrder.DESC)
				.execute().actionGet();
		response.getHits();
	}
	
	/** filter */
	public void filter() {
		FilterBuilder filterBuilder = FilterBuilders
				.andFilter(
					FilterBuilders.existsFilter("title").filterName("exist"),
					FilterBuilders.termFilter("title", "elastic")
						);
		System.out.println(filterBuilder.toString());
		SearchResponse response = client.prepareSearch("library")
				// 这里有点区别
				.setPostFilter(filterBuilder).execute()
				.actionGet();
		response.getHits();
	}
	
	/** facet */
	@SuppressWarnings("deprecation")
	public void facet() {
		
		// 建议使用 aggregation
		FacetBuilder facetBuilder = FacetBuilders.filterFacet("test")
				.filter(FilterBuilders.termFilter("title", "elastic"));
		System.out.println(facetBuilder.toString());
		SearchResponse response = client.prepareSearch("library")
				.addFacet(facetBuilder).execute().actionGet();
		response.getFacets();
	}
	
	/** highlight */
	public void highlight() {
		
		SearchResponse response = client.prepareSearch("wikipedia")
				.addHighlightedField("title")
				.setQuery(QueryBuilders.termQuery("title", "actress"))
				.setHighlighterPreTags("<1>", "<2>")
				.setHighlighterPostTags("</1>","</2>")
				.execute().actionGet();
		response.getHits();
	}
	
	/** suggestion */
	public void suggest() {
		
		SearchResponse response = client.prepareSearch("wikipedia")
				.setQuery(QueryBuilders.matchAllQuery())
				.addSuggestion(new TermSuggestionBuilder("first_suggestion")
						.text("graphics designer")
						.field("_all"))
				.execute().actionGet();
		for (Entry<? extends Option> entry : response.getSuggest().getSuggestion("first_suggestion").getEntries()) {
			
			System.out.println("Check for : " + entry.getText() + " .Options : ");
			for (Option option : entry.getOptions()) {
				System.out.println("\t" + option.getText());
			}
		}
	}
	
	/** count */
	public void count() {
		
		CountResponse response = client.prepareCount("library")
				.setQuery(QueryBuilders.termQuery("title", "elastic"))
				.execute().actionGet();
		response.getCount();
	}
	
	/** scroll */
	public void scroll() {
		
		SearchResponse responseSearch = client.prepareSearch("library")
				.setScroll("1m")
				.setSearchType(SearchType.SCAN)
				.execute().actionGet();
		
		String scrollId = responseSearch.getScrollId();
		SearchResponse response = client.prepareSearchScroll(scrollId)
				.execute().actionGet();
		response.getHits();
	}
	
	/** delete by query */
	public void deleteByQuery() {
		
		DeleteByQueryResponse response = client.prepareDeleteByQuery("library")
				.setQuery(QueryBuilders.termQuery("title", "ElasticSearch"))
				.execute().actionGet();
		
		response.getHeaders();
	}
	
	/** multi get */
	public void multiGet() {
		
		MultiGetResponse response = client.prepareMultiGet()
				.add("library", "book", "1", "2")
				.execute().actionGet();
		response.getResponses();
	}
	
	/** Percolator */
	public void percolator() {
		
		try {
			client.prepareIndex("_percolator", "prcltr", "query:1")
				.setSource(XContentFactory.jsonBuilder()
						.startObject().field("query", QueryBuilders.termQuery("test", "abc"))
						.endObject())
				.execute().actionGet();
		} catch (ElasticsearchException e) {
			
			e.printStackTrace();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
		// query
		try {
			PercolateResponse response = client.preparePercolate()
					.setIndices("_percolator")
					.setDocumentType("prcltr")
					.setSource(XContentFactory.jsonBuilder().startObject()
							.startObject("doc")
							.field("test").value("abc")
							.endObject().endObject())
					.execute().actionGet();
			
			for (Match match : response.getMatches()) {
				System.out.println(match.toString());
			}
		} catch (ElasticsearchException e) {
			
			e.printStackTrace();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	}
	
	/** explain */
	public void explain() {
		
		ExplainResponse response = client.prepareExplain("library", "book", "1")
				.setQuery(QueryBuilders.termQuery("title", "elastic"))
				.execute().actionGet();
		response.getExplanation();
	}
	
	@After
	public void after() {
		
		if (null != client) {
			System.out.println("ElasticSearch close");
			client.close();
		}
	}
}

