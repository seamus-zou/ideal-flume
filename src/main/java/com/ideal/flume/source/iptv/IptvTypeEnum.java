package com.ideal.flume.source.iptv;

import java.util.Arrays;
import java.util.Comparator;

public enum IptvTypeEnum {
	PRODUCT_ORDER(0), VOD(0), TV(0), TVOD(0), VAS(0), KJ(0), PROGRAM_TVOD(0), ACTIVE_USER(0), EPG_GROUP(0), SPACE(
			0), GJ(0), BILL(0),CONTENT_Crawler(0), CONTENT(1), SERVICE(1), PRODUCT(1), PRODUCT_SERVICE(
                    1), SERVICE_CONTENT(1), USER(1), TVMON(1), PRODUCT_ORDER_ALL(2);

	private final int saveType;

	private static final IptvTypeEnum[] sortedValues = values();

	static {
		Arrays.sort(sortedValues, new Comparator<IptvTypeEnum>() {
			@Override
			public int compare(IptvTypeEnum o1, IptvTypeEnum o2) {
				return o2.name().length() - o1.name().length();
			}
		});
	}

	/**
	 * 
	 * @param saveType
	 *            0: 增量入库;1: 只保留一份,有新数据时把老的删掉
	 */
	private IptvTypeEnum(int saveType) {
		this.saveType = saveType;
	}

	public int getSaveType() {
		return saveType;
	}

	public static IptvTypeEnum getTypeByFileName(String fileName) {
		fileName = fileName.toUpperCase();
		for (IptvTypeEnum tp : sortedValues) {
			if (fileName.startsWith(tp.name())) {
				return tp;
			}
		}
		return null;
	}
}
