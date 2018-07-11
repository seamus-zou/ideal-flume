package com.ideal.flume.tools;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class HostnameUtils {
	public static String getHostname() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException uhe) {
			String host = uhe.getMessage();
			if (host != null) {
				int colon = host.indexOf(':');
				if (colon > 0) {
					return host.substring(0, colon);
				}
			}

			try {
				return InetAddress.getLocalHost().getHostAddress();
			} catch (UnknownHostException e) {
				return "unknowhostname" + System.currentTimeMillis();
			}
		}
	}
	
	public static String getIP() {
		try {
			return InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			return "unknowip" + System.currentTimeMillis();
		}
	}
}
