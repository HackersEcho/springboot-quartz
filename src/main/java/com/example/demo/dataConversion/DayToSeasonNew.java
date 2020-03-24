package com.example.demo.dataConversion;

import com.example.demo.utils.LocalDateUtils;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDate;

/**
 * 日数据转季数据
 * @author echo
 *
 */
public class DayToSeasonNew extends DataToConversionFactory{
	public static void main(String[] args) {
		DayToSeasonNew bean = new DayToSeasonNew();
		bean.historySync();
	}
	static {
		tableName = "t_mete_season_data";
		dataType = "季数据";
	}
	@Override
	protected void historySync() {
		String startDate = "20191201";
		while (true) {
			String endDate = LocalDateUtils.stringToDate(startDate).plusMonths(3).
					plusDays(-1).toString().replace("-", "");
			init(startDate,endDate);
			startDate = LocalDateUtils.stringToDate(startDate).plusMonths(3)
					.toString().replace("-", "");
			if (StringUtils.compare(startDate, "20200328")>0) {
				break;
			}
		}
	}
	@Override
	protected void realTimeSync() {
		LocalDate now = LocalDate.now().plusDays(-1);
		int monthValue = now.getMonthValue();
		int year = now.getYear();
		String startDate = "";
		String endDate = "";
		if(monthValue>=3 && monthValue<=5) {
			startDate = year+"0301";
			endDate = year+"0531";
		}else if (monthValue>=6 && monthValue<=8) {
			startDate = year+"0601";
			endDate = year+"0831";
		}else if (monthValue>=9 && monthValue<=11) {
			startDate = year+"0901";
			endDate = year+"1130";
		}else {
			startDate = (year-1)+"1201";
			endDate = year+"0229";
		}
		init(startDate,endDate);
	}
}
