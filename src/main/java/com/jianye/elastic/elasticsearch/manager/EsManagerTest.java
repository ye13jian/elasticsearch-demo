/**
 * Project Name:elasticsearch-demo
 * File Name:EsManagerTest.java
 * Package Name:com.jianye.elastic.elasticsearch.manager
 * Date:2017年6月30日上午9:28:26
 * Copyright (c) 2017, 963552657@qq.com All Rights Reserved.
 *
*/

package com.jianye.elastic.elasticsearch.manager;

import java.util.Map;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.shutdown.NodesShutdownResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.shard.ShardId;
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
public class EsManagerTest {
	
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
	
	/** 集群健康状态  cluster healthy */
	public void clusterHealth() {
		
		ClusterHealthResponse response = client.admin().cluster()
				.prepareHealth("library")
				.execute().actionGet();
		
		response.getHeaders();
	}
	
	/** 集群状态 cluster state */
	public void clusterState() {
		
		ClusterStateResponse response = client.admin().cluster()
				.prepareState()
				.execute().actionGet();
		
		response.getState();
	}
	
	/** 集群配置修改 cluster settings */
	public void updateSetting() {
		
		Map<String, Object> map = Maps.newHashMap();
		map.put("indices.ttl.interval", "10m");
		
		ClusterUpdateSettingsResponse response = client.admin().cluster()
				.prepareUpdateSettings()
				.setTransientSettings(map)
				.execute().actionGet();
		
		response.getTransientSettings();
	}
	
	/** 集群重新路由 */
	public void reroute() {
		
		ClusterRerouteResponse response = client.admin().cluster()
				.prepareReroute()
				.setDryRun(true)
				.add(new MoveAllocationCommand(new ShardId("library", 3), "server69", "server68")
						, new CancelAllocationCommand(new ShardId("library", 2), "server33", true))
				.execute().actionGet();
		
		response.getState();
	}
	
	/** node info */
	public void nodeinfo() {
		
		NodesInfoResponse response = client.admin().cluster()
				.prepareNodesInfo()
				.setNetwork(true)
				.setPlugins(true)
				.execute().actionGet();
		
		response.getClusterName();
	}
	
	/** nodes statistics */
	public void nodestatistics() {
		
		NodesStatsResponse response = client.admin().cluster()
				.prepareNodesStats()
				.all()
				.execute().actionGet();
		
		response.getNodes();
	}
	
	/** nodes hot thread */
	public void nodeHotThread() {
		
		NodesHotThreadsResponse response = client.admin().cluster()
				.prepareNodesHotThreads()
				.execute().actionGet();
		
		response.getNodes();
	}
	
	/** nodes shutdown */
	public void nodeShutdown() {
		
		NodesShutdownResponse response = client.admin().cluster()
				.prepareNodesShutdown()
				.execute().actionGet();
		
		response.getNodes();
	}
	
	/** search shards */
	public void searchshards() {
		
		ClusterSearchShardsResponse response = client.admin().cluster()
				.prepareSearchShards()
				.setIndices("library")
				.setRouting("12")
				.execute().actionGet();
		
		response.getNodes();
	}
	
	@After
	public void after() {
		
		if (null != client) {
			System.out.println("ElasticSearch close");
			client.close();
		}
	}
}

