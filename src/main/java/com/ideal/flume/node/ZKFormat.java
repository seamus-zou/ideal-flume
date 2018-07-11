/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ideal.flume.node;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.flume.lifecycle.LifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * flume zk 节点 初始化 Created by Juntao.Zhang on 2014/10/9.
 */
public class ZKFormat {
	private static final Logger logger = LoggerFactory.getLogger(ZKFormat.class);
	CuratorFramework client;
	public String zkConnString;
	public static final String basePath = "/flume";

	public ZKFormat() {
		File file = new File(ZKFormat.class.getResource("/zoo.cfg").getFile());
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String str;
			while ((str = reader.readLine()) != null) {
				if (!str.startsWith("#") && StringUtils.isNotBlank(str)) {
					zkConnString = str;
					break;
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			if (null != reader) {
				try {
					reader.close();
				} catch (IOException e) {
					logger.error("", e);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		ZKFormat format = new ZKFormat();
		format.init();
	}

	private void init() throws Exception {
		client = createClient();
		client.start();
		if (client.checkExists().forPath(basePath) != null)
			client.delete().deletingChildrenIfNeeded().forPath(basePath);
		initPath();
		readConf();
	}

	private void initPath() throws Exception {
		client.create().forPath(basePath, "this is zk flume dir.".getBytes());
	}

	private void readConf() {
		File dir = new File(ZKFormat.class.getResource("/original").getFile());
		String str;
		if (!dir.exists())
			return;
		File[] files = dir.listFiles();
		if (files == null)
			return;
		for (File f : files) {
			BufferedReader in = null;
			try {
				in = new BufferedReader(new FileReader(f));
				StringBuilder buffer = new StringBuilder();
				while ((str = in.readLine()) != null) {
					buffer.append(str).append("\n");
				}
				client.create().creatingParentsIfNeeded()
						.forPath(basePath + "/" + f.getName(), LifecycleState.STOP.toString().getBytes());
				client.create().creatingParentsIfNeeded()
						.forPath(basePath + "/" + f.getName() + "/conf", buffer.toString().getBytes());
			} catch (Exception e) {
				logger.error("read conf exception {}", e.getMessage(), e);
			} finally {
				if (null != in) {
					try {
						in.close();
					} catch (IOException e) {
						logger.error("", e);
					}
				}
			}
		}
	}

	protected CuratorFramework createClient() {
		return CuratorFrameworkFactory.newClient(zkConnString, new ExponentialBackoffRetry(1000, 1));
	}
}
