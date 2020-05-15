package com.example.demo.dataConversion.climate;


import com.example.demo.utils.CommonHandlerUtils;
import com.example.demo.utils.DBUtil;
import com.example.demo.utils.LocalDateUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class InsertDataToSeasonTable {
	public static final String REGION_OF_WINTER = "常冬区";
	public static final String REGION_OF_NOTMAL = "四季分明区";
	public static final String REGION_OF_SUMMER = "常夏区";
	public static final String REGION_OF_SPRING = "常春区";
	public static final String REGION_NO_WINTER = "无冬区";
	public static final String REGION_NO_SUMMER = "无夏区";
	public static final String defauleDate = "1900-01-01 00:00:00";// 当没有数据是给个默认时间
	private static final Log log = LogFactory.getLog(InsertDataToSeasonTable.class);

	/**
	 * 
	 * @Title: getAllAvgTempDatas 得到该年份区间中每一天的日平均气温
	 * @param year查询年份
	 * @return 参数类型
	 * @return List<Map<String,Object>>
	 * @date: 2018年11月13日 下午3:59:30
	 */

	public static List<Map<String, Object>> getAllAvgTempDatas(int year) {
			String sql = "SELECT t.stationNo,DATE_FORMAT(t.ObserverTime,'%Y-%m-%d %H:%i:%s') as ObserverTime,b.station_name,t.TEM_Avg,DATE_FORMAT(t.ObserverTime,'%m') AS months"
					+ " FROM" + " t_mete_ns_day_data t JOIN t_mete_station b ON b.device_id = t.stationNo"
					+ " WHERE t.stationNo regexp '^[0-9]+$' " + "AND DATE_FORMAT(t.ObserverTime,'%Y') =" + year
					+ " AND t.TEM_Avg BETWEEN -99 AND 99";
		return CommonHandlerUtils.sqlHandle(sql, year + "四季入季数据查询");
	}
	public  static List<Map<String, Object>> getAllStationName() {
		String sql = "SELECT b.station_name AS stationName , b.device_id AS stationNo,b.longitude AS lon,b.latitude AS lat FROM t_mete_station b WHERE b.station_type = 1";
		return CommonHandlerUtils.sqlHandle(sql,  "查询所有站点");
	}
	public static List<Map<String, Object>> getSeasonDatas(int year) {
		List<Map<String, Object>> needDatas = new ArrayList<>();
		List<Map<String, Object>> datas = getConvertSlideDatas(year);
		// 插入时间
		String insertTime = LocalDateTime.now().toString();
		// 得到所有的站点编号和名称
		List<Map<String, Object>> stationNames = getAllStationName();
		// 得到春季入季的数据 常年滑动气温连续五天大于10℃第一个对应时间为春季起始日
		List<Map<String, Object>> springDatas = springDatas(datas);
		// 得到春季入季的数据 常年滑动气温连续五天大于22℃第一个对应时间为夏季起始日
		List<Map<String, Object>> summerDatas = summerDatas(datas);
		// 得到春季入季的数据 常年滑动气温连续五天小于22℃第一个对应时间为秋季起始日
		List<Map<String, Object>> autumnDatas = autumnDatas(datas);
		// 得到春季入季的数据 常年滑动气温连续五天小于10℃第一个对应时间为冬季起始日
		List<Map<String, Object>> winterDatas = winterDatas(datas);
		// 得到所有站点的季节起始日

		for (Map<String, Object> map : stationNames) {
			/*//得到该站点的四季季节的开始日期
			List<Map<String, Object>> pValues = pAllValues.stream()
					.filter(x -> StringUtils.equals(x.get("stationNo").toString(), map.get("stationNo").toString()))
					.collect(Collectors.toList());*/
			Map<String, Object> seasonMap = getSingleSeasonDatas(map.get("stationNo").toString(), springDatas,
					summerDatas, autumnDatas, winterDatas/*, pValues*/);
			seasonMap.put("insertTime", insertTime);
			seasonMap.put("sNo", map.get("stationNo"));
			seasonMap.put("sName", map.get("stationName"));
			seasonMap.put("YEAR", year);
			needDatas.add(seasonMap);
		}
		return needDatas;
	}

	/**
	 * 得到当个站点的每一个四季入季的开始和结束时间
	 * 
	 * @Title: getSingleSeasonDatas
	 * @param stationNo
	 *            站点编号
	 * @param springDatas
	 * @param summerDatas
	 * @param autumnDatas
	 * @param winterDatas
	 * @param pValues该站点编号的季节开始常年值
	 * @return 参数类型
	 * @return Map<String,Object>
	 * @date: 2018年11月14日 上午11:00:49
	 */
	public static Map<String, Object> getSingleSeasonDatas(String stationNo, List<Map<String, Object>> springDatas,
			List<Map<String, Object>> summerDatas, List<Map<String, Object>> autumnDatas,
			List<Map<String, Object>> winterDatas/*, List<Map<String, Object>> pValues*/) {
		Map<String, Object> map = new HashMap<>();
		String spStart = null;// 春季开始时间
		String spEnd = null;// 春季结束时间
		String suStart = null;// 夏开始时间
		String suEnd = null;// 夏季结束时间
		String auStart = null;// 秋季开始时间
		String auEnd = null;// 秋季结束时间
		String wiStart = null;// 冬季开始时间
		String wiEnd = null;// 冬季结束时间
/*//得到夏季和秋季常年值开始日期
		String suP = pValues.stream().filter(x->StringUtils.equals(x.get("season_type").toString(), "2"))
				.collect(Collectors.toList()).get(0).get("event_time").toString();
		String auP = pValues.stream().filter(x->StringUtils.equals(x.get("season_type").toString(), "3"))
				.collect(Collectors.toList()).get(0).get("event_time").toString();*/
		spStart = earliestDate(stationNo, springDatas);
		suStart = earliestDate(stationNo, summerDatas);
		auStart = earliestDate(stationNo, autumnDatas);
		wiStart = earliestDate(stationNo, winterDatas);
		// 春季开始冬季结束时间
		if (StringUtils.isBlank(spStart)) {
			spStart = defauleDate;
			wiEnd = defauleDate;
		} else {
			wiEnd = LocalDateUtils.DateTimeToString(LocalDateUtils.stringToDateTime(spStart).plusDays(-1));
		}
		// 夏季开始春季结束时间
		if (StringUtils.isBlank(suStart)) {
			suStart = defauleDate;
			spEnd = defauleDate;
		} else {
			spEnd = LocalDateUtils.DateTimeToString(LocalDateUtils.stringToDateTime(suStart).plusDays(-1));
		}
		// 秋季开始夏季结束时间
		if (StringUtils.isBlank(auStart)) {
			auStart = defauleDate;
			suEnd = defauleDate;
		} else {
			suEnd = LocalDateUtils.DateTimeToString(LocalDateUtils.stringToDateTime(auStart).plusDays(-1));
		}
		// 冬季开始秋季结束时间
		if (StringUtils.isBlank(wiStart)) {
			wiStart = defauleDate;
			auEnd = defauleDate;
		} else {
			auEnd = LocalDateUtils.DateTimeToString(LocalDateUtils.stringToDateTime(wiStart).plusDays(-1));
		}
		map.put("spStart", spStart);
		map.put("suStart", suStart);
		map.put("auStart", auStart);
		map.put("wiStart", wiStart);
		map.put("spEnd", spEnd);
		map.put("suEnd", suEnd);
		map.put("auEnd", auEnd);
		map.put("wiEnd", wiEnd);
		return map;
	}

	/**
	 * 得到集合中时间最早的数据，返回那个时间
	 * 
	 * @Title: earliestDate
	 * @param stationNo
	 * @param datas
	 * @return 参数类型
	 * @return String
	 * @date: 2018年11月14日 上午11:58:31
	 */
	public static String earliestDate(String stationNo, List<Map<String, Object>> datas) {
		String time = null;
		try {
			List<Map<String, Object>> seasons = datas.parallelStream()
					.filter(x -> x.get("stationNo").toString().equals(stationNo)).collect(Collectors.toList());
			time = seasons.parallelStream().map(x -> x.get("ObserverTime").toString())
					.sorted((s1, s2) -> s1.compareTo(s2)).collect(Collectors.toList()).get(0);
		} catch (Exception e) {
			return null;
		}
		return time;
	}

	// 得到春季入季的数据
	public static List<Map<String, Object>> springDatas(List<Map<String, Object>> datas) {
		List<Map<String, Object>> rangeDatas = datas.parallelStream()
				.filter(x -> Integer.valueOf(x.get("months").toString()) >= 2
						&& Integer.valueOf(x.get("months").toString()) <= 5)
				.collect(Collectors.toList());
		List<Map<String, Object>> springDatas = new ArrayList<>();
		for (Map<String, Object> map : rangeDatas) {
			String time1 = map.get("ObserverTime").toString();
			String sNo1 = map.get("stationNo").toString();
			String temAvg = map.get("TEM_Avg").toString();
			boolean flag = true;
			// 将时间由字符串转化为LocalDateTime
			LocalDateTime dateTime1 = LocalDateUtils.stringToDateTime(time1);
			if (Double.valueOf(temAvg) >= 10 && Double.valueOf(temAvg) < 99) {
				for (int i = 1; i <= 4; i++) {
					LocalDateTime dateTime2 = dateTime1.plusDays(i);
					// 将时间转化为String
					String time = LocalDateUtils.DateTimeToString(dateTime2);
					// 得到该站当前时间的前i天的日平均气温
					String tempTem = null;
					try {
						tempTem = datas.parallelStream()
								.filter(x -> x.get("ObserverTime").toString().equals(time)
										&& x.get("stationNo").toString().equals(sNo1))
								.collect(Collectors.toList()).get(0).get("TEM_Avg").toString();
					} catch (Exception e) {
						flag = false;
						break;
					}
					if (Double.valueOf(tempTem) < 10) {
						flag = false;
						break;
					}
				}
			} else {
				flag = false;
			}
			if (flag) {
				springDatas.add(map);
			}

		}
		return springDatas;
	}

	// 得到夏季入季的数据
	public static List<Map<String, Object>> summerDatas(List<Map<String, Object>> datas) {
		List<Map<String, Object>> rangeDatas = datas.parallelStream()
				.filter(x -> Integer.valueOf(x.get("months").toString()) >= 4
						&& Integer.valueOf(x.get("months").toString()) <= 9)
				.collect(Collectors.toList());
		List<Map<String, Object>> summerDatas = new ArrayList<>();
		for (Map<String, Object> map : rangeDatas) {
			String time1 = map.get("ObserverTime").toString();
			String sNo1 = map.get("stationNo").toString();
			String temAvg = map.get("TEM_Avg").toString();
			boolean flag = true;
			// 将时间由字符串转化为LocalDateTime
			LocalDateTime dateTime1 = LocalDateUtils.stringToDateTime(time1);
			if (Double.valueOf(temAvg) > 21.5 && Double.valueOf(temAvg) < 99) {
				for (int i = 1; i <= 4; i++) {
					LocalDateTime dateTime2 = dateTime1.plusDays(i);
					// 将时间转化为String
					String time = LocalDateUtils.DateTimeToString(dateTime2);
					// 得到该站当前时间的前i天的日平均气温
					String tempTem = null;
					try {
						tempTem = datas.parallelStream()
								.filter(x -> x.get("ObserverTime").toString().equals(time)
										&& x.get("stationNo").toString().equals(sNo1))
								.collect(Collectors.toList()).get(0).get("TEM_Avg").toString();
					} catch (Exception e) {
						flag = false;
						break;
					}
					if (Double.valueOf(tempTem) < 21.5) {
						flag = false;
						break;
					}
				}
			} else {
				flag = false;
			}
			if (flag) {
				summerDatas.add(map);
			}

		}
		return summerDatas;
	}

	// 得到秋季入季的数据
	public static List<Map<String, Object>> autumnDatas(List<Map<String, Object>> datas) {
		List<Map<String, Object>> rangeDatas = datas.parallelStream()
				.filter(x -> Integer.valueOf(x.get("months").toString()) >= 7
						&& Integer.valueOf(x.get("months").toString()) <= 11)
				.collect(Collectors.toList());
		List<Map<String, Object>> autumnDatas = new ArrayList<>();
		for (Map<String, Object> map : rangeDatas) {
			String time1 = map.get("ObserverTime").toString();
			String sNo1 = map.get("stationNo").toString();
			String temAvg = map.get("TEM_Avg").toString();
			boolean flag = true;
			// 将时间由字符串转化为LocalDateTime
			LocalDateTime dateTime1 = LocalDateUtils.stringToDateTime(time1);
			if (Double.valueOf(temAvg) < 22) {
				for (int i = 1; i <= 4; i++) {
					LocalDateTime dateTime2 = dateTime1.plusDays(i);
					// 将时间转化为String
					String time = LocalDateUtils.DateTimeToString(dateTime2);
					// 得到该站当前时间的前i天的日平均气温
					String tempTem = null;
					try {
						tempTem = datas.parallelStream()
								.filter(x -> x.get("ObserverTime").toString().equals(time)
										&& x.get("stationNo").toString().equals(sNo1))
								.collect(Collectors.toList()).get(0).get("TEM_Avg").toString();
					} catch (Exception e) {
						flag = false;
						break;
					}
					if (Double.valueOf(tempTem) >= 22) {
						flag = false;
						break;
					}
				}
			} else {
				flag = false;
			}
			if (flag) {
				autumnDatas.add(map);
			}

		}
		return autumnDatas;
	}

	// 得到冬季入季的数据
	public static List<Map<String, Object>> winterDatas(List<Map<String, Object>> datas) {
		List<Map<String, Object>> rangeDatas = datas.parallelStream()
				.filter(x -> Integer.valueOf(x.get("months").toString()) >= 9
						&& Integer.valueOf(x.get("months").toString()) <= 12)
				.collect(Collectors.toList());
		List<Map<String, Object>> winterDatas = new ArrayList<>();
		for (Map<String, Object> map : rangeDatas) {
			String time1 = map.get("ObserverTime").toString();
			String sNo1 = map.get("stationNo").toString();
			String temAvg = map.get("TEM_Avg").toString();
			boolean flag = true;
			// 将时间由字符串转化为LocalDateTime
			LocalDateTime dateTime1 = LocalDateUtils.stringToDateTime(time1);
			if (Double.valueOf(temAvg) < 10) {
				for (int i = 1; i <= 4; i++) {
					LocalDateTime dateTime2 = dateTime1.plusDays(i);
					// 将时间转化为String
					String time = LocalDateUtils.DateTimeToString(dateTime2);
					// 得到该站当前时间的前i天的日平均气温
					String tempTem = null;
					try {
						tempTem = datas.parallelStream()
								.filter(x -> x.get("ObserverTime").toString().equals(time)
										&& x.get("stationNo").toString().equals(sNo1))
								.collect(Collectors.toList()).get(0).get("TEM_Avg").toString();
					} catch (Exception e) {
						flag = false;
						break;
					}
					if (Double.valueOf(tempTem) >= 10) {
						flag = false;
						break;
					}
				}
			} else {
				flag = false;
			}
			if (flag) {
				winterDatas.add(map);
			}

		}
		return winterDatas;
	}

	/**
	 * 将平均气温转化为滑动平均气温
	 * 
	 * @Title: getConvertDatas
	 * @param year
	 *            参数类型
	 * @return void
	 * @date: 2018年11月14日 上午9:03:19
	 */
	public static List<Map<String, Object>> getConvertSlideDatas(int year) {
		List<Map<String, Object>> datas = getAllAvgTempDatas(year);
		List<Map<String, Object>> convertDatas = new ArrayList<>();
		// 将常年平均气温序列转化为常年滑动平均气温
		for (Map<String, Object> map : datas) {
			String time1 = map.get("ObserverTime").toString();
			String sNo1 = map.get("stationNo").toString();
			String tempAvg = map.get("TEM_Avg").toString();
			// 将时间由字符串转化为LocalDateTime
			LocalDateTime dateTime1 = LocalDateUtils.stringToDateTime(time1);
			// 得到月份
			int month = dateTime1.getMonth().getValue();
			// 如果月份大于等于三将平均气温转化为滑动平均气温
			if (month >= 2) {
				Double sum = Double.valueOf(tempAvg);
				// 得到前四天的数据
				LocalDateTime lacalDate1 = dateTime1.plusDays(-1);
				String data1 = LocalDateUtils.DateTimeToString(lacalDate1);// 前一天
				LocalDateTime lacalDate2 = dateTime1.plusDays(-2);
				String data2 = LocalDateUtils.DateTimeToString(lacalDate2);// 前两天
				LocalDateTime lacalDate3 = dateTime1.plusDays(-3);
				String data3 = LocalDateUtils.DateTimeToString(lacalDate3);// 前三天
				LocalDateTime lacalDate4 = dateTime1.plusDays(-4);
				String data4 = LocalDateUtils.DateTimeToString(lacalDate4);// 前四天
				try {// 处理数据没有的情况 例如：14.7+13.6+10.2+9.3+7.9 =55.7 0403
					sum += datas.parallelStream()
							.filter(x -> x.get("stationNo").toString().equals(sNo1)
									&& (x.get("ObserverTime").toString().equals(data1)
											|| x.get("ObserverTime").toString().equals(data2)
											|| x.get("ObserverTime").toString().equals(data3)
											|| x.get("ObserverTime").toString().equals(data4)))
							.mapToDouble(y -> Double.valueOf(y.get("TEM_Avg").toString())).sum();
				} catch (Exception e) {
					sum += 9999d;
				}
				double temValue = sum / 5;
				DecimalFormat df = new DecimalFormat("#.00");
				// 直接map赋值会改变原有的数据结构
				Map<String, Object> tempMap = new HashMap<>();
				tempMap.put("stationNo", map.get("stationNo"));
				tempMap.put("station_name", map.get("station_name"));
				tempMap.put("ObserverTime", map.get("ObserverTime"));
				tempMap.put("months", map.get("months"));
				tempMap.put("TEM_Avg", df.format(temValue));
				convertDatas.add(tempMap);
			}

		}
		return convertDatas;
	}

	/**
	 * 将数据插入表中
	 * 
	 * @param startYear
	 * @param endYear
	 */
	public static void insertTotable(int startYear, int endYear){
		Connection connection = DBUtil.getConnection();
		for (int year = startYear; year <= endYear; year++) {
			List<Map<String, Object>> maplist = getSeasonDatas(year);
			Map<String, Object> delFieldAndVal = new HashMap<String, Object>();
			delFieldAndVal.put("YEAR", year);
			try {
				DBUtil.delData2DB("t_mete_climate_four_season", delFieldAndVal, connection);
				DBUtil.inserBatchtData2DB("t_mete_climate_four_season", maplist, connection);
				System.out.println(year + "年四季入季数据入库成功");
			} catch (Exception e) {
				System.out.println(year + "年四季入季日数据入库失败");
				e.printStackTrace();
			}
		}
		try {
			connection.close();
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	public static void main(String[] args){
		LocalDate nowDate = LocalDate.now();
		int startYears = nowDate.getYear();
		int endYears = nowDate.plusDays(-1).getYear();
		InsertDataToSeasonTable.insertTotable(startYears, endYears);
	}
}
