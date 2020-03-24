package com.example.demo.dataConversion;

/**
 * @author echo
 * 候 旬 月 季 年数据同步的统一入口
 */
public class DataEntranceSync {
	public static void main(String[] args) {
		init(new DayToMonthNew());// 月数据转化同步
		init(new DayToSeasonNew());
		init(new DayToYearNew());
	}

	public static void init(DataToConversionFactory bean) {
		bean.realTimeSync();
	}
	
}
