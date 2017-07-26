/**
 * Project Name:elasticsearch-demo
 * File Name:EsRetriveTest2.java
 * Package Name:com.jianye.elastic.elasticsearch.retrive
 * Date:2017年6月30日下午3:17:45
 * Copyright (c) 2017, 963552657@qq.com All Rights Reserved.
 *
*/

package com.jianye.elastic.elasticsearch.retrive;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.After;
import org.junit.Before;

/**
 * ClassName:EsRetriveTest4 <br/>
 * Function: aggregation聚合查询. <br/>
 * Reason:	 TODO ADD REASON. <br/>
 * Date:     2017年6月30日 下午3:17:45 <br/>
 * @author   jianye
 * @version  
 * @since    JDK 1.6
 * @see 	 
 */
public class EsRetriveTest4 {
	
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
		
		
	}
	
	@After
	public void after() {
		
		if (null != client) {
			System.out.println("ElasticSearch close");
			client.close();
		}
	}
}

