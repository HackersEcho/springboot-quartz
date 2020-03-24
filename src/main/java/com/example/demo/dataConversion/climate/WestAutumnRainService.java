package com.example.demo.dataConversion.climate;


import com.example.demo.utils.DBUtil;
import com.example.demo.utils.DoubleMathUtil;
import com.example.demo.utils.LocalDateUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

/**
   *      华西秋雨
 * @author echo
 *
 */
public class WestAutumnRainService {
	public static ClimateDao dao = new ClimateDao();
	public static String stations = "'53929','53938','53941','53944','53945','53946','53947','53948','53949','53950','53955','57003','57016','57020','57021','57022','57023','57024','57025','57026','57027','57028','57029','57030','57031','57032','57033','57034','57035','57037','57038','57039','57040','57041','57042','57043','57044','57045','57046','57047','57048','57049','57054','57055','57057','57106','57113','57119','57123','57124','57126','57127','57128','57129','57131','57132','57134','57137','57140','57143','57144','57153','57154','57155','57211','57213','57231','57232','57233','57238','57242','57245','57247','57248','57254','57343'";
	public static Connection conn = DBUtil.getConnection();
	public static Map<String, String> regionMap = new HashMap<>();
	public static void main(String[] args) throws SQLException {
		for (int i = 1961; i <=2019; i++) {
		 init(i+"");
		}
//		init("2019");
	}
	static {
		regionMap.put("6"," 榆林");
		regionMap.put("7"," 延安");
		regionMap.put("8"," 咸阳");
		regionMap.put("9"," 渭南");
		regionMap.put("10"," 铜川");
		regionMap.put("11"," 宝鸡");
		regionMap.put("12"," 商洛");
		regionMap.put("13"," 汉中");
		regionMap.put("14"," 安康");
		regionMap.put("15"," 西安");
	}
	/**
	 * 帮客户提取秋雨时间段的数据
	 */
	public static void statistic() {
		List<Map<String, Object>> precipitation = dao.getPrecipitation("20190909", "20190918", stations);
		List<Map<String, Object>> resList = new ArrayList<>();
		for (int i = 20190909; i <=20190918; i++) {
			Map<String, Object> map = new HashMap<>();
			String time = i+"";
			double asDouble = precipitation.stream().filter(x->x.get("observerTime").toString().equals(time)).mapToDouble(
					x->Double.parseDouble(x.get("pre").toString())).sum();
			map.put(time, DoubleMathUtil.round(asDouble, 1));
			resList.add(map);
		}
		System.out.println(resList);
	} 
	public static void init(String year) throws SQLException {
		List<Map<String, Object>> autumnList = dao.getAutumnList(year,stations);
		// 得到所有的秋雨日和非秋雨日
		Map<String, String> westAutumnDays = getWestAutumnDays(autumnList);
		String rainyPeriod = getRainyPeriod(westAutumnDays);
		packageData(rainyPeriod,year);
		westRainData("1961-1990");
		westRainData("1971-2000");
		westRainData("1981-2010");
		conn.close();	
		// 得到所有的多雨期
		System.out.println(rainyPeriod);
		
	}
	private static void packageData(String rainyPeriod,String year) {
		String startDate = "--";// 开始时间
		String endDate = "--";// 结束
		String insertTime = LocalDate.now().toString().replace("-","");
		String rainLen = "--";// 秋雨期长度
		String rainAmount = "--";// 华西秋雨量	
		
		if (StringUtils.isNotBlank(rainyPeriod)) {
			String[] periods = rainyPeriod.split("@");
			startDate = periods[0].split("-")[0];
			endDate = periods[periods.length-1].split("-")[1];
			Long dayLen = LocalDateUtils.getDifferDays(startDate, endDate);
			if (dayLen == 4) {// 特殊情况
				dayLen = 6L;
				endDate = LocalDateUtils.stringToDate(endDate).plusDays(2).toString().replace("-", "");
			}
			rainLen = dayLen+"";
			
			List<Map<String, Object>> precipitations = dao.getPrecipitation(startDate, endDate, stations);
			double value = precipitations.stream().mapToDouble(x->Double.parseDouble(x.get("pre").toString())).sum()/(76);
			rainAmount = DoubleMathUtil.round(value, 1)+"";
		}
		
		Map<String, Object> insertMap = new HashMap<>();
			insertMap.put("year", year);
			insertMap.put("insertTime", insertTime);
			insertMap.put("rainyPeriod", rainyPeriod);
			insertMap.put("startDate", startDate);
			insertMap.put("endDate", endDate);
			insertMap.put("rainLen", rainLen);
			insertMap.put("rainAmount", rainAmount);
		Map<String, Object> delMap = new HashMap<>();
			delMap.put("year", year);
		
		// 数据入库
		dataToLibrary(delMap,insertMap,"huaxi_autumn_rain",year+"华西秋雨统计数据插入成功");
		
	}
	/**
	 * 因历史未出现该情况的发生，暂时未做
	 * @return
	 */
	public static String specialAutumnYear() {
		return null;
	}
	/**
	 * 得到所有的华西秋雨日
	 * @param autumnList
	 * @author echo
	 * @return
	 */
	private static 	Map<String, String> getWestAutumnDays(List<Map<String, Object>> autumnList) {
		int standard = 76/2;
		Map<String, String> resMap = new HashMap<>();
		// 得到所有的时间
		List<String> timeList = autumnList.stream().map(x->x.get("observerTime").toString()).distinct().collect(Collectors.toList());
		
		StringJoiner rainDays = new StringJoiner("@");
		StringJoiner nonDays = new StringJoiner("@");
		
		for (String time : timeList) {
			long count = autumnList.stream().filter(x->StringUtils.equals(time, x.get("observerTime").toString())
					&& Double.parseDouble(x.get("pre").toString())>=0.1).count();
			if (count >= standard) {
				rainDays.add(time);
			}else {
				nonDays.add(time);
			}
		}
		resMap.put("rainDays", rainDays.toString());
		resMap.put("nonDays", nonDays.toString());
		
		return resMap;
	}
	
	/**
	 * 统计秋雨开始和结束
	 * @param rainMap 
	 * @author echo
	 * @return
	 */
	private static 	String getRainyPeriod(Map<String, String> rainMap) {
		List<String> startLit =  new ArrayList<>();
		List<String> endLit =  new ArrayList<>();
		// 得到秋雨日时间,并按时间的从小到大排序
		String[] rainDays = rainMap.get("rainDays").split("@");
		// 得到所有满足条件的多雨期开始日期
		List<String> rainDaysList = Arrays.asList(rainDays).stream().sorted().collect(Collectors.toList());
		int rSize = rainDaysList.size();
		for (int i = 0; i < rSize; i++) {
			if (dateCheck(rainDaysList.get(i),rainDaysList)) {
				startLit.add(rainDaysList.get(i));
				i+=4;
			}
		}			

		// 所有满足条件的多雨期结束日期
		String[] nonDays = rainMap.get("nonDays").split("@");
			List<String> nonDaysList = Arrays.asList(nonDays).stream().sorted().collect(Collectors.toList());
			int nSize = nonDaysList.size();
			for (int i = 0; i < nSize; i++) {
				if (dateCheck(nonDaysList.get(i),nonDaysList)) {
					endLit.add(nonDaysList.get(i));
					i+=4;
				}
			}			
		// 根据一个开始日期对应一个结束日期,提炼出所有的多雨期时间段。用字符串@分割
			StringJoiner sJoiner = new StringJoiner("@");
			Map<String, String> tempMap = new HashMap<>();
			for (String sDate : startLit) {
				for (String eDate : endLit) {
					if (StringUtils.compare(sDate, eDate)<0) {
						if (tempMap.containsKey(eDate)) {
							break;
						}else {
							tempMap.put(eDate, eDate);
							sJoiner.add(sDate+"-"+eDate);
							break;
						}
					}else {
						continue;
					}
				}
			}
			// 如果最后一个开始日期没有结束日期，那么结束日期以当前时间为准。也就是库中查出来时间最大的时间
			if (startLit.size()>0) {
				boolean flag = StringUtils.compare(startLit.get(startLit.size()-1), endLit.get(endLit.size()-1))>0;
				if (flag) {
					// 确定开始
					String sta = endLit.get(endLit.size()-1);
					int index = 0;
					for (int i = 0; i < startLit.size(); i++) {
						if (StringUtils.compare(startLit.get(i), sta)<0) {
							index++;
						}else {
							break;
						}
					}
					String start = startLit.get(index);
					// 确定结束
					String str = rainDaysList.get(rainDaysList.size()-1);
					String end = LocalDateUtils.stringToDate(str).plusDays(1).toString().replace("-", "");		
					sJoiner.add(start+"-"+end);
				}
			}	
		return sJoiner.toString();
	}
	/**
	 * 验证日期中出现连续5个的时间(可2-4中的一个时间可以没有)
	 * @param date yyyyMMdd
	 * @param dateList 时间集合
	 * @return
	 */
	private static boolean dateCheck(String date,List<String> dateList) {
		boolean flag = false;
		// 得到传入时间的第五日
		String date5 = LocalDateUtils.stringToDate(date).plusDays(4).toString().replace("-", "");
		if (dateList.contains(date)&&dateList.contains(date5)) {
			// 2-4天必须满足在数据中的数量大于等于2
			int count = 0;
			for (int i = 1; i <= 3; i++) {
				String dateTemp = LocalDateUtils.stringToDate(date).plusDays(i).toString().replace("-", "");
				if (dateList.contains(dateTemp)) {
					count++;
				}
			}
			if (count>=2) {
				flag = true;
			}
		}
		return flag;
	}
	
	
	// ============================================================
	/**
	 * 
	 * @param startYear yyyy
	 * @param endYear
	 * @param climateScale 常年值区间
	 * @author echo
	 * @serialData 2019/09/18
	 * @return
	 */
	public static List<Map<String, Object>> westRainData(String climateScale){
		List<Map<String, Object>> resultList = new ArrayList<>();
		List<Map<String, Object>> westRainData = dao.westRainData();
		String stations = "'53929','53938','53941','53944','53945','53946','53947','53948','53949','53950','53955','57003','57016','57020','57021','57022','57023','57024','57025','57026','57027','57028','57029','57030','57031','57032','57033','57034','57035','57037','57038','57039','57040','57041','57042','57043','57044','57045','57046','57047','57048','57049','57054','57055','57057','57106','57113','57119','57123','57124','57126','57127','57128','57129','57131','57132','57134','57137','57140','57143','57144','57153','57154','57155','57211','57213','57231','57232','57233','57238','57242','57245','57247','57248','57254','57343'";	
		String scale1 = climateScale.split("-")[0];
		String scale2 = climateScale.split("-")[1];
		List<Map<String, Object>> collect = westRainData.stream().filter(x->StringUtils.compare(x.get("year").toString(), scale1)>=0
				&&StringUtils.compare(x.get("year").toString(), scale2)<=0).collect(Collectors.toList());
		int valid = collect.size();// 常年值中实际发生华西秋雨的年数
		// 华西秋雨期长度的气候平均值
		double lenPerennial = DoubleMathUtil.round(collect.stream().mapToDouble(x->Double.parseDouble(
				x.get("rainLen").toString())).average().getAsDouble(),1);
		// 得到所有的长度集合并按照从大到小的顺序排序
		List<Double> lenList = westRainData.stream().map(x->Double.parseDouble(x.get("rainLen").toString()))
				.sorted(Comparator.reverseOrder()).collect(Collectors.toList());
		// 华西秋雨量的气候平均值
		double amountPerennial = DoubleMathUtil.round(collect.stream().mapToDouble(x->Double.parseDouble(
				x.get("rainAmount").toString())).average().getAsDouble(),1);
			 
		List<Double> amountList = westRainData.stream().map(x->Double.parseDouble(x.get("rainAmount").toString()))
				.sorted(Comparator.reverseOrder()).collect(Collectors.toList());
	
		// 秋雨常年开始和结束时间
		LocalDate now = LocalDate.of(2000,1,1);
		int sDays = collect.stream().mapToInt(x->LocalDateUtils.stringToDate(x.get("startDate").toString()).getDayOfYear()).sum()/valid;
			String startPerennial = now.withDayOfYear(sDays).toString().replace("-", "").substring(4);
		int eDays = collect.stream().mapToInt(x->LocalDateUtils.stringToDate(x.get("endDate").toString()).getDayOfYear()).sum()/valid;
			String endPerennial = now.withDayOfYear(eDays).toString().replace("-", "").substring(4);
		// 得到秋雨长度气候均方差和秋雨量的气候均方差
		double sl = collect.stream().mapToDouble(x->Math.pow(Double.parseDouble
						(x.get("rainLen").toString())-lenPerennial, 2)).sum()/valid/76;
		double sr = collect.stream().mapToDouble(x->Math.pow(Double.parseDouble
				(x.get("rainAmount").toString())-amountPerennial, 2)).sum()/valid/76;
		
		List<String> yearList = westRainData.stream().map(x->x.get("year").toString()).distinct().collect(Collectors.toList());
		for (String year : yearList) {
			String startDate = "--";// 华西秋雨开始时间
			String endDate = "--";
			double rainLen = 0.0; // 秋雨长度
			double rainAmount = 0.0;// 秋雨量
			String startToAvg = "--";// 开始时间距平
			String endToAvg = "--";// 结束时间距平
			String rainyPeriod = "--";// 多雨期时间段
			String rainyPeriodAndVal = "--";// 多雨期时间段和降水量
			String lenRank = "--";// 长度排名
			String amountRank = "--";// 秋雨量排名
			double rainLenIndex = 0.0;// 长度指数
			double rainAmountIndex = 0.0;// 秋雨量指数
			double compositeIndex = 0.0;// 综合强度指数
			String lenGrade = "--"; // 长度强度等级
			String amountGrade = "--"; // 秋雨量强度等级
			String compositeGrade = "--"; // 综合强度等级
			double stations50 = 0.0; // 暴雨站次
			double extremValue = 0.0;// 日最大降水量
			String extremValueInfos = "--";//日最大降水量包括对应的自动站和对应的行政区
			
			Optional<Map<String, Object>> op = westRainData.stream().filter(x->x.get("year").toString().equals(year+"")).findFirst();
			if (op.isPresent()) {
				Map<String, Object> map = op.get();
				startDate = map.get("startDate").toString();
				endDate = map.get("endDate").toString();
				rainyPeriod = map.get("rainyPeriod").toString();
				rainLen = Double.parseDouble(map.get("rainLen").toString());
				rainAmount = Double.parseDouble(map.get("rainAmount").toString());
				startToAvg = (LocalDateUtils.stringToDate(startDate).getDayOfYear() - sDays)+"";
				endToAvg = (LocalDateUtils.stringToDate(endDate).getDayOfYear() - eDays)+"";
				lenRank = lenList.indexOf(rainLen)+1+"";
				amountRank = amountList.indexOf(rainAmount)+1+"";
				rainLenIndex = DoubleMathUtil.div(rainLen - lenPerennial,sl ,1);
				rainAmountIndex = DoubleMathUtil.div(rainAmount - amountPerennial,sr,1) ;
				compositeIndex = DoubleMathUtil.div(rainLenIndex+rainAmountIndex, 2, 1);
				lenGrade = westAutumnPartition(rainLenIndex);
				amountGrade = westAutumnPartition(rainAmountIndex);
				compositeGrade = westAutumnPartition(compositeIndex);
				// 得到暴雨站次和日最大降水量(对应的自动站名和行政区名)
				List<Map<String, Object>> singleWest = dao.getPrecipitation(year+"0821", year+"1101", stations);
				List<Map<String, Object>> rainStormList = filterListByTime(singleWest,startDate,endDate);
				stations50 = rainStormList.stream().filter(x->Double.parseDouble(x.get("pre").toString())>=50).count();
				extremValue = rainStormList.stream().mapToDouble(x->Double.parseDouble(x.get("pre").toString())).max().getAsDouble();
				double mVal = extremValue;
				Map<String, Object> map2 = rainStormList.parallelStream().filter(x->Double.parseDouble(
						x.get("pre").toString())==mVal).findFirst().get();
				extremValueInfos = extremValue+"("+regionMap.get(map2.get("region").toString())+"-"+map2.get("stationName")+"-"+map2.get("observerTime")+")";
				// 得到多雨期每个时间段的降水量
				String[] periods = rainyPeriod.split("@");
				StringJoiner sJoiner = new StringJoiner(";");
				for (String period : periods) {
					String sT = period.split("-")[0];
					String eT = period.split("-")[1];
					List<Map<String, Object>> data = filterListByTime(singleWest,sT,eT);
					double val = data.stream().mapToDouble(x->Double.parseDouble(x.get("pre").toString())).sum()/76;
					period = sT.substring(4)+"-"+eT.substring(4);
					sJoiner.add(period+"/"+DoubleMathUtil.round(val, 1));
				}
				rainyPeriodAndVal = sJoiner.toString();
				
				Map<String, Object> rMap = new HashMap<>();
				rMap.put("year", year);
				rMap.put("startDate", startDate);
				rMap.put("startPerennial", startPerennial); //  秋雨常年值开始时间
				rMap.put("startToAvg", startToAvg);
				rMap.put("endDate", endDate);
				rMap.put("endPerennial", endPerennial);
				rMap.put("endToAvg", endToAvg);
				rMap.put("rainLen", rainLen);
				rMap.put("lenPerennial", lenPerennial);
				rMap.put("avgLen", DoubleMathUtil.sub(rainLen, lenPerennial,1));
				rMap.put("lenRank", lenRank);
				rMap.put("rainLenIndex", rainLenIndex);
				rMap.put("lenGrade", lenGrade);
				rMap.put("rainAmount", rainAmount);
				rMap.put("amountPerennial", amountPerennial);
				rMap.put("avgAmount", DoubleMathUtil.div(rainAmount-amountPerennial,amountPerennial/100,1));// 秋雨量距平百分率
				rMap.put("amountRank", amountRank);
				rMap.put("rainAmountIndex", rainAmountIndex);
				rMap.put("amountGrade", amountGrade);
				rMap.put("compositeIndex", compositeIndex);
				rMap.put("compositeGrade", compositeGrade);
				rMap.put("rainyPeriodAndVal", rainyPeriodAndVal);
				rMap.put("stations50", stations50);
				rMap.put("extremValue", extremValue);
				rMap.put("extremValueInfos", extremValueInfos);
				rMap.put("climateScale", climateScale);
				// 得到所有站的数据
				List<Map<String, Object>> allRainInfo = dao.getAllRainInfo(startDate,endDate);
				// 暴雨站次
				long contry50 = allRainInfo.stream().filter(x -> Double.valueOf(x.get("PRE_Time_2020").toString()) >= 50).count();
				// 最大日降水量
				double contryMax = allRainInfo.stream().mapToDouble(x -> Double.valueOf(x.get("PRE_Time_2020").toString()))
						.max().getAsDouble();
				// 得到最大日降水量对应的站点名称
				Map<String, Object> map3 = allRainInfo.stream().filter(x -> Double.valueOf(
						x.get("PRE_Time_2020").toString()) == contryMax).findFirst().get();
				String contryMaxName =regionMap.get(map3.get("region").toString())+"-"+map3.get("stationName").toString()+"-"+map3.get("observerTime").toString();
				// 秋雨量
				long count = allRainInfo.stream().map(x->x.get("stationNo")).distinct().count();
				double contryValue = allRainInfo.stream().mapToDouble(x -> Double.valueOf(x.get("PRE_Time_2020").toString())).sum()/count;
				// 保留一位小数
				DecimalFormat df = new DecimalFormat("#.0");
				rMap.put("contry50", contry50);
				rMap.put("contryMax", contryMax);
				rMap.put("contryValue", df.format(contryValue));
				rMap.put("contryMaxName", contryMaxName);
				
				Map<String, Object> delMap = new HashMap<>();
				delMap.put("year", year);
				delMap.put("climateScale", climateScale);
				// 入库
				dataToLibrary(delMap,rMap,"inquire_huaxi_rain_table",year+"华西秋雨表格数据入库成功");
			}	
			
		}
		return resultList;
	}
	/**
	 * 得到开始结束时间得到自己想要的集合
	 * @param data
	 * @param startDate yyyyMMdd
	 * @param endDtae
	 * @return
	 */
	private static List<Map<String, Object>> filterListByTime(List<Map<String, Object>> data,String startDate,String endDtae){
		return data.parallelStream().filter(x->StringUtils.compare(x.get("observerTime").toString(), startDate)>=0
				&& StringUtils.compare(x.get("observerTime").toString(), endDtae)<=0).collect(Collectors.toList());
	}
	/**
	 *  华西秋雨强度等级划分
	 * @param I1 华西秋雨长度指数
	 * @param I2 秋雨量指数
	 * @param I3 综合强度指数
	 * @author echo
	 * @serialData 2019/09/18
	 * @return
	 */
	private static String westAutumnPartition(double I) {
		if (I >= 1.5) {
			return "显著偏强";
		}
		if (I >= 0.5 && I < 1.5) {
			return "偏强";
		}
		if (I > -0.5 && I < 0.5) {
			return "正常";
		}
		if (I > -1.5 && I <= -0.5) {
			return "偏弱";
		}
			return "显著偏弱";	
	}
	/**
	 * 将数据入库，已有的删除在入
	 * @param delMap 根据主键删除
	 * @param insertMap 插入数据map
	 * @param tableName 表名
	 * @param infos 打印到控制台的信息
	 */
 private static void dataToLibrary(Map<String, Object> delMap,Map<String, Object> insertMap,String tableName,String infos) {
	 try {
			DBUtil.delData2DB(tableName,delMap, conn);
			DBUtil.insertData2DB(tableName, insertMap, conn);
			System.out.println(infos);
		} catch (Exception e) {
			e.printStackTrace();
		}
 }
}
