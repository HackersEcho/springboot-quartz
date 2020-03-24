package com.example.demo.dataConversion;

import com.example.demo.utils.LocalDateUtils;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDate;
import java.time.temporal.TemporalAdjusters;


public class DayToMonthNew extends DataToConversionFactory{
	public static void main(String[] args) {
		DayToMonthNew bean = new DayToMonthNew();
		bean.historySync();
	}
	static {
		tableName = "t_mete_month_data";
		dataType = "月数据";
	}
	@Override
	protected void historySync() {
		 String date = LocalDate.now().toString().replace("-", "");
		String startDate = "20200101";
		while (true) {
			String endDate = LocalDateUtils.stringToDate(startDate).plusMonths(1).
					plusDays(-1).toString().replace("-", "");
			init(startDate,endDate);
			startDate = LocalDateUtils.stringToDate(startDate).plusMonths(1)
					.toString().replace("-", "");
			if (StringUtils.compare(startDate, date)>0) {
				break;
			}
		}
	}
	@Override
	protected void realTimeSync() {
		LocalDate now = LocalDate.now().plusDays(-1);
		// 取本月第1天：
		String startDate = now.with(TemporalAdjusters.firstDayOfMonth()).toString();
		// 取本月最后一天，再也不用计算是28，29，30还是31：
		String endDate = now.with(TemporalAdjusters.lastDayOfMonth()).toString(); 
		init(startDate,endDate);
	}
	
}
