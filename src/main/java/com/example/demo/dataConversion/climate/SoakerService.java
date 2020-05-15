package com.example.demo.dataConversion.climate;

import com.example.demo.utils.CommonHandlerUtils;
import com.example.demo.utils.DoubleMathUtil;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 首场透雨日界定
 * 
 * @author LZ
 * @author LMQ 透雨标准变更 20180414
 * @author LMQ 透雨标准变更 20180508
 */
public class SoakerService {

	public static void main(String[] args) {
		new SoakerService().getRainDateBYFisrtCondition("2019");
	}
	public List<Map<String, Object>> getRainDataByTimeRect(String year) {
		String sql = "select t.stationNo,t.PRE_Time_2020,t.ObserverTime from t_mete_ns_day_data t WHERE\n" +
				" t.MonthDay BETWEEN 301 AND 430 AND t.Years = "+year+" and  t.PRE_Time_2020>=0.1 and t.PRE_Time_2020<=999";
		return CommonHandlerUtils.sqlHandle(sql,year+"首场透雨");
	}
	// 定义：3月1日-4月30日，（1）第一次出现日降水量大于或等于15毫米，（2）或过程降水量大于或等于20毫米的降水量定义为首场透雨
	// 1.查询数据
	// 2.筛选不满足条件（1）的站点
	// 3.对不满足条件（1）站点进行条件（2）界定
	// 4.计算符合条件的首场透雨相关指标数据
	// 5.数据入库
	public void getRainDateBYFisrtCondition(String dateYear) {
		List<Map<String, Object>> maps = getRainDataByTimeRect(dateYear);
		if (maps.size() > 0) {
			// 1.1出现降雨的站点编号集合
			List<String> stationList1 = maps.parallelStream().map(x -> x.get("stationNo").toString()).distinct()
					.collect(Collectors.toList());
			// 1.2筛选出满足（1）的数据集合
			List<Map<String, Object>> maptempList = maps.parallelStream()
					.filter(x -> Double.valueOf(x.get("PRE_Time_2020").toString()) >= 15).collect(Collectors.toList());
			if (maptempList.size() > 0) {
				// 1.2.1得到指标集合
				List<Map<String, Object>> result1 = calculationTarget_SatisfyConditionOne(maptempList);
				// 1.2.2数据入库
				resultInserDB(result1);
			}
			// 1.3筛选出满足（1）的站点集合
			List<String> stationList2 = maptempList.parallelStream().map(x -> x.get("stationNo").toString()).distinct()
					.collect(Collectors.toList());
			// 1.3.1全部满足条件（1）
			if (stationList1.size() == stationList2.size()) {
				return;
			}
			// 1.4集合做差筛选出未满足（1）的站点集合
			stationList1.removeAll(stationList2);
			// 1.4.1差集，即为未满足（1）的站点集合
			List<String> stationList3 = stationList1;
			List<Map<String, Object>> result2 = calculationTarget_SatisfyConditionTwo(maps, stationList3);
			// 1.4.2数据入库
			resultInserDB(result2);
		}
	}

	/**
	 * 计算满足条件（1）的站点的透雨各项指标
	 * 
	 * @param list        需要计算的数据集合
	 * @param stationList 满足（1）的站点集合
	 */
	private List<Map<String, Object>> calculationTarget_SatisfyConditionOne(List<Map<String, Object>> list) {
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		// 1.1出现降雨的站点编号集合
		List<String> stationList = list.parallelStream().map(x -> x.get("stationNo").toString()).distinct()
				.collect(Collectors.toList());
		// 1.按站点遍历每个站点透雨日期，透雨量等指标

		for (String sno : stationList) {
			// 1.1筛选当前站点集合
			List<Map<String, Object>> listCurrt = list.parallelStream()
					.filter(x -> x.get("stationNo").toString().equals(sno)).collect(Collectors.toList());
			// 1.2集合按照日期排序
			listCurrt.sort((a, b) -> a.get("ObserverTime").toString().compareTo(b.get("ObserverTime").toString()));
			// 1.3得到当前站点的数据对象
			Map<String, Object> mapCurrt = listCurrt.get(0);
			Map<String, Object> mapCurrtStation = new HashMap<String, Object>();
			mapCurrtStation.put("stationNo", sno);
			mapCurrtStation.put("event_year", mapCurrt.get("ObserverTime").toString().split("-")[0]);
			mapCurrtStation.put("event_time", mapCurrt.get("ObserverTime").toString().split("-")[1] + ""
					+ mapCurrt.get("ObserverTime").toString().split("-")[2].substring(0, 2));
			mapCurrtStation.put("pre", mapCurrt.get("PRE_Time_2020"));
			mapCurrtStation.put("insertTime", LocalDateTime.now().toString());
			resultList.add(mapCurrtStation);
		}
		return resultList;
	}

	/**
	 * 计算满足条件（2）的站点的透雨各项指标
	 * 
	 * @param list        需要计算的数据集合
	 * @param stationList 未满足（1）的站点集合
	 */
	private List<Map<String, Object>> calculationTarget_SatisfyConditionTwo(List<Map<String, Object>> list,
			List<String> stationList) {
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		// 1.按站点遍历每个站点透雨日期，透雨量等指标
		for (String sno : stationList) {
			// 1.1筛选得到当前站点连续降雨日期
			List<Map<String, Object>> listCurrt = list.parallelStream()
					.filter(x -> x.get("stationNo").toString().equals(sno)).collect(Collectors.toList());
			// 1.2集合按照日期排序
			listCurrt.sort((a, b) -> a.get("ObserverTime").toString().compareTo(b.get("ObserverTime").toString()));
			// 1.3筛选当前站点集合的日期并判断是否是连续日期
			List<String> dateList = listCurrt.parallelStream().map(x -> x.get("ObserverTime").toString())
					.collect(Collectors.toList());
			// 1.3.1筛选连续的日期
			// 1.3.1.1当前站点不同时间段集合
			List<Map<String, Object>> dataListMap = new ArrayList<Map<String, Object>>();
			LocalDate startTime = null;
			LocalDate endTime = null;
			DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd");
			// 1.3.1.2筛选连续的日期
			for (int i = 0; i < dateList.size(); i++) {
				LocalDate startTimeTemp = LocalDate.parse(dateList.get(i).split(" ")[0], df);
				LocalDate endTimeTemp = null;
				if (i < dateList.size() - 1) {
					endTimeTemp = LocalDate.parse(dateList.get(i + 1).split(" ")[0], df);
					if (endTimeTemp.toEpochDay() - startTimeTemp.toEpochDay() == 1) {
						if (startTime == null) {
							startTime = startTimeTemp;
						}
					} else {
						if (startTime != null) {
							endTime = startTimeTemp;
							Map<String, Object> map = new HashMap<String, Object>();
							map.put("startTime", startTime);
							map.put("endTime", endTime);
							dataListMap.add(map);
							startTime = null;
						} else {
							continue;
						}
					}
				}
			}
			// 1.4根据不同时间段结合筛选每个时间段的累计降水量是否达到条件（2）
			// 标识，第一个时间段满足条件（2）就不在进行二次判断否则进行二次判断，以此类推
			boolean falg = false;
			for (Map<String, Object> map : dataListMap) {
				List<Map<String, Object>> tempList = listCurrt.parallelStream().filter(x -> {
					LocalDate startDate = LocalDate.parse(map.get("startTime").toString(), df);
					LocalDate endDate = LocalDate.parse(map.get("endTime").toString(), df);
					LocalDate dateX = LocalDate.parse(x.get("ObserverTime").toString().split(" ")[0], df);
					if ((dateX.toEpochDay() - startDate.toEpochDay()) >= 0
							&& (dateX.toEpochDay() - endDate.toEpochDay() <= 0)) {
						return true;
					} else {
						return false;
					}
				}).collect(Collectors.toList());
				DoubleSummaryStatistics avgTempStatistics = tempList.parallelStream()
						.mapToDouble((x) -> Double.parseDouble(x.get("PRE_Time_2020").toString())).summaryStatistics();
				if (falg) {
					break;
				}
				if (avgTempStatistics.getSum() >= 20) {
					falg = true;
					Map<String, Object> mapCurrtStation = new HashMap<String, Object>();
					mapCurrtStation.put("stationNo", sno);
					mapCurrtStation.put("event_year", map.get("startTime").toString().split("-")[0]);
					mapCurrtStation.put("event_time", map.get("startTime").toString().split("-")[1] + ""
							+ map.get("startTime").toString().split("-")[2]);
					mapCurrtStation.put("pre", DoubleMathUtil.round(avgTempStatistics.getSum(), 1));
					mapCurrtStation.put("insertTime", LocalDateTime.now().toString());
					resultList.add(mapCurrtStation);
				}
			}
		}
		return resultList;
	}

	/**
	 * 计算满足条件（2）的站点的透雨各项指标
	 * 
	 * @param list 需要计算的数据集合
	 */
	private void resultInserDB(List<Map<String, Object>> list) {
		for (Map<String, Object> map : list) {
			Map<String, Object> del = new HashMap<String, Object>();
			del.put("stationNo", map.get("stationNo"));
			del.put("event_year", map.get("event_year"));
			CommonHandlerUtils.dataToLibrary(del,map,"t_mete_climate_first_soaker","首场透雨数据同步");
		}
	}
}
