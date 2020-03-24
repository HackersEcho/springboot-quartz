package com.example.demo.dataConversion;

import com.example.demo.utils.LocalDateUtils;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDate;
import java.time.temporal.TemporalAdjusters;


public class DayToYearNew extends DataToConversionFactory{
	public static void main(String[] args) {
		DayToYearNew bean = new DayToYearNew();
		bean.historySync();
	}
	static {
		tableName = "t_mete_year_data ";
		dataType = "年数据";
	}
	@Override
	protected void historySync() {
		String date = LocalDate.now().toString().replace("-", "");
		String startDate = "19510101";
		while (true) {
			String endDate = LocalDateUtils.stringToDate(startDate).plusYears(1).
					plusDays(-1).toString().replace("-", "");
			init(startDate,endDate);
			startDate = LocalDateUtils.stringToDate(startDate).plusYears(1)
					.toString().replace("-", "");
		if (StringUtils.compare(startDate, date)>0) {
				break;
			}
		}
	}
	@Override
	protected void realTimeSync() {
		LocalDate now = LocalDate.now().plusDays(-1);
		// 取本年第1天：
		String startDate = now.with(TemporalAdjusters.firstDayOfYear()).toString();
		// 取本年最后一天
		String endDate = now.with(TemporalAdjusters.lastDayOfYear()).toString(); 
		init(startDate,endDate);
	}
	
}
