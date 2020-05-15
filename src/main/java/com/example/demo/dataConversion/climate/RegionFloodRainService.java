package com.example.demo.dataConversion.climate;

import com.example.demo.utils.CommonHandlerUtils;
import com.example.demo.utils.DoubleMathUtil;

import java.text.DateFormat;
import java.text.ParseException;
import java.time.Year;
import java.util.*;
import java.util.stream.Collectors;

public class RegionFloodRainService {
	public static void main(String[] args) throws ClassNotFoundException {
		init();
	}
	public static void init(){
		try {
			String year = Year.now().toString();
			List<Map<String, Object>> resultAll = getRegionFloodRainProcess(year, year, "1981-2010");
			for (Map<String, Object> map : resultAll) {
				Map<String, Object> mapDelete= new HashMap<String, Object>();
				mapDelete.put("year", map.get("year"));
				CommonHandlerUtils.dataToLibrary(mapDelete,map,"t_mete_floodrain_history",year+"初夏汛雨");
			}
		}catch (Exception e){
			e.printStackTrace();
		}
	}
	public static List<Map<String, Object>> getfloodRain(String startDate, String endDate, String stationIdsStr, String field) {
		String sql = "SELECT t.stationNo, DATE_FORMAT(t.ObserverTime,'%Y%m%d') as ObserverTime, t." + field
				+ " as val, a.station_name FROM t_mete_ns_day_data t  JOIN t_mete_station  a ON t.stationNo=a.device_id  "
				+ " WHERE   DATE_FORMAT(t.ObserverTime,'%Y')>= " + startDate
				+ " AND   DATE_FORMAT(t.ObserverTime,'%Y')<= " + endDate + "  AND\n "
				+ "DATE_FORMAT(t.ObserverTime,'%m%d')>=0501  AND DATE_FORMAT(t.ObserverTime,'%m%d')<=0830  AND  t."
				+ field + " > 0.01  AND  t." + field + "\n" + " <=  9999  AND t.stationNo in (" + stationIdsStr
				+ ")   ORDER BY t.ObserverTime    ";
		return CommonHandlerUtils.sqlHandle(sql, "初夏汛雨");
	}
	/**
	 * 初夏汛雨区域过程统计
	 * 
	 * @param startDate
	 *            开始时间
	 * @param endDate
	 *            结束时间
	 * @param minThreshold
	 *            最小值
	 * @param maxThreshold
	 *            最大值
	 * @param days
	 *            持续天数
	 * @return
	 */
	public static List<Map<String, Object>> getRegionFloodRainProcess(String startDate, String endDate,
																	  String climateScale) {
		DateFormat format = new java.text.SimpleDateFormat("yyyyMMdd");
		String stationStr = "'53845','53854','53857','53929','53931','53938','53942','53945','53946','53947','53948','53949','53955','57003','57016','57021','57022','57023','57024','57025','57026','57027','57028','57029','57030','57031','57032','57033','57034','57035','57038','57039','57042','57043','57044','57045','57046','57047','57048','57049','57054','57057','57106','57113','57119','57124','57126','57127','57129','57132','57134','57137','57143','57144','57153','57154','57155','57211','57231','57232','57233','57238','57242','57245','57248','57254','57343'";
		// 1.将所有符合条件的应信息结果压入一个集合里面
		List<Map<String, Object>> liveValue = getfloodRain("1951",
				format.format(new Date().getTime()).substring(0, 4), stationStr, "PRE_Time_2020");
		// 2.根据年份计算汛雨数据
		List<Map<String, Object>> resultAll = getBaseValues(startDate.replace("-", ""), endDate.replace("-", ""),
				liveValue, format);
		// 3.计算常年值的数据集合
		List<Map<String, Object>> resultClimateAll = getBaseValues("19810521", "20100830", liveValue, format);
		Map<String, Object> climateMap = getClimateValues(resultClimateAll, climateScale);
		// 3.通过筛选的结果计算相应指标并返回客户端
		for (Map<String, Object> map : resultAll) {
			String grade = "";// 汛雨量等级
			Double climateStrong = 0.0; // 综合强度指数
			Double zp = (Double.valueOf(map.get("rainSum").toString())
					- Double.valueOf(climateMap.get("climateRainSum").toString()))
					/ Double.valueOf(climateMap.get("climateSqrt").toString()) ;
			if (zp >= 1.5)
				grade = "显著偏多";
			else if (zp < 1.5 && zp >= 0.5)
				grade = "偏多";
			else if (zp < 0.5 && zp > -0.5)
				grade = "正常";
			else if (zp <= -0.5 && zp > -1.5)
				grade = "偏少";
			else
				grade = "显著偏少";
			double 区域雨季内平均日降水强度 = Double.valueOf(map.get("climateRainSumAll").toString())
					/ Double.valueOf(map.get("rainLength").toString());
			double 区域雨季内平均日降水强度气候平均值 = Double.valueOf(climateMap.get("climateRainSumAllAVG").toString());
			climateStrong = DoubleMathUtil
					.round(Integer.valueOf(map.get("rainLength").toString())
							/ Double.valueOf(climateMap.get("climateLength").toString()), 2)
					+ (区域雨季内平均日降水强度 / 区域雨季内平均日降水强度气候平均值 / 2)
					+ DoubleMathUtil.round(Double.valueOf(map.get("climateRainSumAll").toString())
							/ Double.valueOf(climateMap.get("climateRainSumAll").toString()), 2)
					- 2.5;

			String climateStrongGrade = "";// 汛雨量等级
			if (climateStrong >= 1.25)
				climateStrongGrade = "强";
			else if (climateStrong < 1.25 && climateStrong >= 0.375)
				climateStrongGrade = "偏强";
			else if (climateStrong < 0.375 && climateStrong > -0.375)
				climateStrongGrade = "正常";
			else if (climateStrong <= -0.375 && climateStrong > -1.25)
				climateStrongGrade = "偏弱";
			else
				climateStrongGrade = "弱";
			//距平百分率
			double   rainSumAny = DoubleMathUtil.round((Double.valueOf(map.get("rainSum").toString())
					- Double.valueOf(climateMap.get("climateRainSum").toString()))/Double.valueOf(climateMap.get("climateRainSum").toString()), 1)*100;
			map.put("zp", DoubleMathUtil.round(zp, 1));
			map.put("lengthAny", DoubleMathUtil.round(Integer.valueOf(map.get("rainLength").toString())
					- Double.valueOf(climateMap.get("climateLength").toString()), 1));
			map.put("rainSumAny", rainSumAny);
			map.put("rainSumGrade", grade);
			map.put("climateStrong", DoubleMathUtil.round(climateStrong, 1));
			map.put("climateStrongGrade", climateStrongGrade);
		}
		return resultAll;
	}

	/**
	 * 求汛雨常年值数据
	 * 
	 * @param resultAll
	 * @param climateScale
	 * @return
	 */
	private static Map<String, Object> getClimateValues(List<Map<String, Object>> resultAll, String climateScale) {
		// 遍历结果集合计算并筛选符合条件的结果
		Map<String, Object> climateMap = new HashMap<>();

		int climateScaleStart = Integer.valueOf(climateScale.substring(0, 4));
		int climateScaleEnd = Integer.valueOf(climateScale.substring(5, 9));

		Double climateLength = 0.0; // Lo：某区域雨季的气候平均长度（日数）；
		Double climateRainSumAll = 0.0; // Ro：某区域雨季内监测站总降水量气候平均值；
		Double climateRainSumAllAVG = 0.0; // (R/L)o：某区域雨季平均日降水强度的气候平均值
		Double climateRainSum = 0.0; // Po：表示汛雨降水量的气候平均值；
		Double climateSqrt = 0.0; // Sp：表示汛雨降水量的气候均方差
		// 根据常年值的尺度帅选结果集合
		List<Map<String, Object>> climateScaleList = resultAll.stream()
				.filter(x -> (Integer.valueOf(x.get("year").toString()) >= climateScaleStart
						&& Integer.valueOf(x.get("year").toString()) <= climateScaleEnd))
				.collect(Collectors.toList());
		for (Map<String, Object> map : climateScaleList) {
			climateLength += Integer.valueOf(map.get("rainLength").toString());
			climateRainSumAll += Double.valueOf(map.get("climateRainSumAll").toString());
			climateRainSum += DoubleMathUtil.round(Double.valueOf(map.get("climateRainSumAll").toString()) / 67, 2);
			climateRainSumAllAVG += DoubleMathUtil.round(Double.valueOf(map.get("climateRainSumAll").toString())
					/ Integer.valueOf(map.get("rainLength").toString()), 2);
		}
		climateRainSumAll = DoubleMathUtil.round(climateRainSumAll / 30.0, 2);
		climateRainSum = DoubleMathUtil.round(climateRainSum / 30.0, 2);

		for (Map<String, Object> map : climateScaleList) {
			climateSqrt += Math.pow(Double.valueOf(map.get("rainSum").toString()) - climateRainSum, 2);
		}
		climateSqrt = DoubleMathUtil.round(Math.sqrt(climateSqrt / 30.0), 2);

		climateLength = DoubleMathUtil.round(climateLength / 30.0, 2);

		climateMap.put("climateLength", climateLength);
		climateMap.put("climateRainSumAll", climateRainSumAll);
		climateMap.put("climateRainSumAllAVG",DoubleMathUtil.round(climateRainSumAllAVG / 30.0, 2) );
		climateMap.put("climateRainSum", climateRainSum);
		climateMap.put("climateSqrt", climateSqrt);
		return climateMap;
	}

	/**
	 * 根据起止时间计算汛雨过程
	 * 
	 * @param startDate
	 * @param endDate
	 * @param liveValue
	 * @param format
	 */

	private static List<Map<String, Object>> getBaseValues(String startDate, String endDate,
			List<Map<String, Object>> liveValue, DateFormat format) {
		List<Map<String, Object>> resultAll = new ArrayList<>();
		// 1. 计算有几年
		int yearCount = Integer.valueOf(endDate.substring(0, 4)) - Integer.valueOf(startDate.substring(0, 4)) + 1;
		// 2. 当前年份
		int currYear = Integer.valueOf(startDate.substring(0, 4));
		// 3.按年份遍历查询
		for (int i = 0; i < yearCount; i++) {

			Calendar currStart = new GregorianCalendar(currYear, 05, 21);
			Calendar calEnd = null;
			// 4.确定过程开、结束始期
			Map<String, String> maptime = getTimePross(currStart, calEnd, currYear, liveValue, format);

			String starttime = maptime.get("starttime");
			String endtime = maptime.get("endtime");

			// 5.判断这一年有无汛雨数据
			if (starttime.equals("")) {
				currYear += 1;
				continue;
			}

			// 6.当前年份
			String currYearStrStart = currYear + "0501";
			String currYearStrEnd = currYear + "0830";
			List<Map<String, Object>> liveValueTemp = liveValue.stream()
					.filter(x -> Integer.valueOf(x.get("ObserverTime").toString()) >= Integer.valueOf(currYearStrStart)
							&& Integer.valueOf(x.get("ObserverTime").toString()) <= Integer.valueOf(currYearStrEnd))
					.collect(Collectors.toList());
			// 6.确定过程开始日期
			starttime = getTime(liveValueTemp, starttime, -1);
			endtime = getTime(liveValueTemp, endtime, 1);
			// 7.计算汛雨长度
			int rainLength = 0;
			try {
				rainLength = (int) ((format.parse(endtime).getTime() - format.parse(starttime).getTime())
						/ (1000 * 60 * 60 * 24)) + 1;
			} catch (ParseException e) {
				e.printStackTrace();
			}
			int starttimeStr = Integer.valueOf(starttime);
			int endtimeStr = Integer.valueOf(endtime);
			List<Map<String, Object>> modellist = liveValue.stream()
					.filter(x -> (Integer.valueOf(x.get("ObserverTime").toString()) >= starttimeStr
							&& Integer.valueOf(x.get("ObserverTime").toString()) <= endtimeStr))
					.collect(Collectors.toList());
			Double rainSum = 0.0;
			for (Map<String, Object> map : modellist) {
				rainSum += Double.valueOf(map.get("val").toString());
			}

			// 8.计算暴雨集合
			List<Map<String, Object>> rainMaxDaysList = liveValue.stream()
					.filter(x -> (Integer.valueOf(x.get("ObserverTime").toString()) >= starttimeStr
							&& Integer.valueOf(x.get("ObserverTime").toString()) <= endtimeStr)
							&& Double.valueOf(x.get("val").toString()) >= 50)
					.collect(Collectors.toList());
			// 9.对日期进行去重，求天数
			List<String> listtime = new ArrayList<>();
			for (Map<String, Object> map : rainMaxDaysList) {
				if (!listtime.contains(map.get("ObserverTime"))) {
					listtime.add(map.get("ObserverTime").toString());
				}
			}

			List<Map<String, Object>> rainMaxList = liveValue.stream()
					.filter(x -> (Integer.valueOf(x.get("ObserverTime").toString()) >= starttimeStr
							&& Integer.valueOf(x.get("ObserverTime").toString()) <= endtimeStr))
					.collect(Collectors.toList());
			Double maxRain = 0.0;

			for (Map<String, Object> map : rainMaxList) {
				maxRain = maxRain < Double.valueOf(map.get("val").toString())
						? Double.valueOf(map.get("val").toString()) : maxRain;
			}
			double max = maxRain;
			String maxRainStation = rainMaxList.stream().filter(x->Double.valueOf(x.get("val").toString())==max).collect(Collectors.toList()).get(0).get("station_name").toString();
			
			// 10.将基本参数放入一个结果集合
			Map<String, Object> mapChild = new HashMap<>();
			mapChild.put("year", currYear); // 汛雨开始年份
			mapChild.put("startTime", starttime.subSequence(4, 8)); // 汛雨开始时间
			mapChild.put("endTime", endtime.subSequence(4, 8)); // 汛雨结束时间
			mapChild.put("rainLength", rainLength); // 汛雨长度
			mapChild.put("climateRainSumAll", DoubleMathUtil.round(rainSum, 1));// 讯雨雨季总降水量
			mapChild.put("rainSum", DoubleMathUtil.round(rainSum / 67, 1));// 讯雨量
			mapChild.put("rainMaxDays", listtime.size()); // 暴雨天数
			mapChild.put("stationCounts", rainMaxDaysList.size()); // 暴雨站次
			mapChild.put("maxRain", maxRain); // 日最大降水量
			mapChild.put("maxRainStation", maxRainStation); // 日最大降水量对应的站点
			resultAll.add(mapChild);
			currYear += 1;
		}
		return resultAll;
	}

	/**
	 * 返回初夏汛雨的开始日和结束日
	 * 
	 * @param currStart
	 * @param calEnd
	 * @param currYear
	 * @param liveValue
	 * @param format
	 * @return
	 */
	private static Map<String, String> getTimePross(Calendar currStart, Calendar calEnd, int currYear,
			List<Map<String, Object>> liveValue, DateFormat format) {
		Map<String, String> time = new HashMap<>();
		String starttime = "";
		List<Integer> endtime = new ArrayList<>();

		// 1.从6.21开始筛选过程开始日直到7.20日结束
		for (int j = 0; j < 30; j++) {

			String currStartMonth = currStart.get(Calendar.MONTH) > 9 ? currStart.get(Calendar.MONTH) + 1 + ""
					: "0" + (currStart.get(Calendar.MONTH) + 1);
			String currStartDay = currStart.get(Calendar.DAY_OF_MONTH) > 9 ? currStart.get(Calendar.DAY_OF_MONTH) + ""
					: "0" + currStart.get(Calendar.DAY_OF_MONTH);

			int start = Integer.valueOf(currYear + currStartMonth + currStartDay);
			calEnd = new GregorianCalendar(currYear, currStart.get(Calendar.MONTH),
					currStart.get(Calendar.DAY_OF_MONTH));
			calEnd.add(Calendar.DAY_OF_MONTH, 2);
			String currEndMonth = calEnd.get(Calendar.MONTH) > 9 ? calEnd.get(Calendar.MONTH) + 1 + ""
					: "0" + (calEnd.get(Calendar.MONTH) + 1);
			String currEndDay = calEnd.get(Calendar.DAY_OF_MONTH) > 9 ? calEnd.get(Calendar.DAY_OF_MONTH) + ""
					: "0" + calEnd.get(Calendar.DAY_OF_MONTH);
			int end = Integer.valueOf(currYear + currEndMonth + currEndDay);

			// 2.筛选本次滑动三天的数据
			List<Map<String, Object>> modelTemp = liveValue.stream()
					.filter(x -> (Integer.valueOf(x.get("ObserverTime").toString()) >= start
							&& Integer.valueOf(x.get("ObserverTime").toString()) <= end)
							&& Double.valueOf(x.get("val").toString()) >= 25)
					.collect(Collectors.toList());
			// 3.判断开始时间是否已经确定，如果确定则查找结束时间，否则继续查找开始时间
			if (starttime.equals("")) {
				if (modelTemp.size() >= 67 / 3) {
					// 判断是那一天达到条件
					starttime = startTime(modelTemp, start + "", format, 25);

				} else {
					modelTemp = liveValue.stream()
							.filter(x -> (Integer.valueOf(x.get("ObserverTime").toString()) >= start
									&& Integer.valueOf(x.get("ObserverTime").toString()) <= end)
									&& Double.valueOf(x.get("val").toString()) >= 50)
							.collect(Collectors.toList());
					if (modelTemp.size() >= 3) {
						starttime = startTime(modelTemp, start + "", format, 50);
					}
				}

			} else {

				// 判断是否大于当年7月20日，大于返回，否则继续
				if (start >= Integer.valueOf(currYear + "0720"))
					break;
				if (modelTemp.size() >= 67 / 3) {
					String temp = startTime(modelTemp, start + "", format, 25);
					int starttimetemp = Integer.valueOf(temp);
					endtime.add(starttimetemp);

				} else {
					modelTemp = liveValue.stream()
							.filter(x -> (Integer.valueOf(x.get("ObserverTime").toString()) >= start
									&& Integer.valueOf(x.get("ObserverTime").toString()) <= end)
									&& Double.valueOf(x.get("val").toString()) >= 50)
							.collect(Collectors.toList());
					if (modelTemp.size() >= 3) {
						String temp = startTime(modelTemp, start + "", format, 50);
						int starttimetemp = Integer.valueOf(temp);
						endtime.add(starttimetemp);
					}
				}
			}
			currStart.add(Calendar.DAY_OF_MONTH, 1);
		}
		// 特殊情况：开始和结束期相差一天
		if (!starttime.equals("") && endtime.size() == 0) {
			Calendar endtimes = new GregorianCalendar(Integer.valueOf(starttime.substring(0, 4)),
					Integer.valueOf(starttime.substring(4, 6)) - 1, Integer.valueOf(starttime.substring(6, 8)));
			endtimes.add(Calendar.DAY_OF_MONTH, 1);
			endtime.add(Integer.valueOf(format.format(endtimes.getTime())));
		}
		time.put("starttime", starttime);
		time.put("endtime", endtime.size() > 0 ? Collections.max(endtime).toString() : "");
		return time;
	}

	/**
	 * 递归判断是那一天达到
	 * 
	 * @param modelTemp
	 * @param start
	 * @return
	 */
	static String starttimetemp = "";

	private static String startTime(List<Map<String, Object>> modelTemp, String start, DateFormat format, int valuse) {
		String starttime = "";
		starttimetemp = start;
		Calendar startCal = new GregorianCalendar(Integer.valueOf(start.substring(0, 4)),
				Integer.valueOf(start.substring(4, 6)) - 1, Integer.valueOf(start.substring(6, 8)));

		for (int i = 0; i < 3; i++) {

			List<Map<String, Object>> model = modelTemp.stream()
					.filter(x -> (x.get("ObserverTime").toString()).equals(starttimetemp)
							&& Double.valueOf(x.get("val").toString()) >= valuse)
					.collect(Collectors.toList());
			if (starttime.equals("")) {
				if (model.size() == 0) {
					startCal.add(Calendar.DAY_OF_MONTH, 1);
					starttimetemp = start = format.format(startCal.getTime());
				} else {
					starttime = format.format(startCal.getTime());
				}
			} else {
				break;
			}
		}
		return starttime;
	}

	/**
	 * 求汛雨开始日期
	 * 
	 * @param liveValue
	 * @param startTime
	 * @return
	 */
	private static String getTime(List<Map<String, Object>> liveValue, String startTime, int falg) {
		String time = "";
		Calendar cal = new GregorianCalendar(Integer.valueOf(startTime.substring(0, 4)),
				Integer.valueOf(startTime.substring(4, 6)) - 1, Integer.valueOf(startTime.substring(6, 8)));
		DateFormat format = new java.text.SimpleDateFormat("yyyyMMdd");
		// 1.根据falg 判断向前或者向后查找区域汛雨过程开始日期或结束日期
		for (int j = 0; j < 30; j++) {
			cal.add(Calendar.DAY_OF_MONTH, falg);
			int start = Integer.valueOf(format.format(cal.getTime()));

			List<Map<String, Object>> modelTemp = liveValue.stream()
					.filter(x -> (Integer.valueOf(x.get("ObserverTime").toString()) == start)
							&& Double.valueOf(x.get("val").toString()) > 0.01)
					.collect(Collectors.toList());
			// 1.将站点放入单独的集合中

			if (falg == -1) {
				if (modelTemp.size() < 67 / 3) {
					cal.add(Calendar.DAY_OF_MONTH, 1);
					break;
				}
			} else {
				int count = 67 - modelTemp.size();
				if (count >= 67 * 2 / 3) {
					cal.add(Calendar.DAY_OF_MONTH, -1);
					break;
				}
			}

		}
		time = format.format(cal.getTime());

		return time;
	}

}