package com.example.demo.dataConversion.climate;


import com.example.demo.utils.CommonHandlerUtils;
import com.example.demo.utils.DoubleMathUtil;
import com.example.demo.utils.LocalDateUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

/**
   * 伏旱统计
 * @1采取两张表  一张表存放每年开始和结束时间 、伏旱期累计降水强度指数和、伏旱天数
 * @2第二张表得到客户需要的数据
 * @author echo
 *
 */
public class VoltDroughtDataTable {
	public static ClimateDao dao = new ClimateDao();
//	public static Connection conn = DBUtil.getConnection("com.mysql.cj.jdbc.Driver",
//			"jdbc:mysql://10.172.14.20:6612/sxcc?serverTimezone=Asia/Shanghai&characterEncoding=utf8&useUnicode=true&useSSL=false",
//			"root", "root");
	private static List<Map<String, Object>> voltDroughtData;
	private String year;// 统计年份
	private String climateScale;// 常年值区间
	static {
		voltDroughtData = dao.getVoltDroughtData();
	}
	public static void main(String[] args) throws Exception {
		VoltDroughtDataTable bean = new VoltDroughtDataTable();
//		bean.historyInit();
//		bean.errorCorrection("20180701", "20180715", "2018");
		bean.customHistoryData();
	}
//
	public void init() {
		String year = LocalDate.now().getYear()+"";
		getData(year, "1961-1990");
		getData(year, "1971-2000");
		getData(year, "1981-2010");
	}
	public void historyInit() {
		Map<String, Object> map = new HashMap<>();
		for (int i = 1955; i < 2020; i++) {
			getData(i+"", "1961-1990");
			getData(i+"", "1971-2000");
			getData(i+"", "1981-2010");
		}
		System.out.println(map);
	}

	private void getData(String year, String climateScale) {
		this.year = year;
		this.climateScale = climateScale;
		// 得到每一天的区域降水强度指数R0
		List<Map<String, Object>> regionIndexList = regionPrecipitationIndex(voltDroughtData);
		// 区域降水强度数据入库
		regionIndexToLibrary(regionIndexList);
		
		String droughtPeriod = droughtPeriod(regionIndexList);
		// 通过时间段确定开始和结束时间
		String periods = determineTime(droughtPeriod,regionIndexList);
		//		System.out.println("droughtPeriod:"+droughtPeriod+"===periods:"+periods);
		packageData(periods,regionIndexList);
		
	}
	/**
	 * 客户所给的伏旱历年开始时间
	 * @throws Exception 
	 */
	public void customHistoryData() throws Exception {
		File file = new File("C:\\Users\\huawei\\Desktop\\drought.txt");
		List<String> fileList = FileUtils.readLines(file);
		for (String strs : fileList) {
			String[] split = strs.split("\t");
			String year = split[0];
			String startDate = "";
			String endDate = "";
			if (StringUtils.isNotBlank(split[1])) {
				startDate = year+nConvertStr(split[1].split("月")[0])+nConvertStr(split[1].split("月")[1].split("日")[0]);
				endDate = year+nConvertStr(split[2].split("月")[0])+nConvertStr(split[2].split("月")[1].split("日")[0]);
			}
//			System.out.println("year:"+year+"\t"+"startDate:"+startDate+"\t"+"endDate:"+endDate);
			errorCorrection(startDate,endDate,year);
		}
	}
	private String nConvertStr(String str) {
		if (str.length()==1) {
			return 0+str;
		}
		return str;
	}
	/**@Description 因为伏旱数据波动很大  需要进行人工订正
	 * @param startDate
	 * @param endDate
	 * @param year
	 * @time 2020年1月10日 下午3:18:20
	 * @author echo
	 */
	public boolean errorCorrection(String startDate,String endDate,String year) {
		List<Map<String, Object>> regionIndexList = dao.getVoltDroughtPSIData(year);
		List<String> climateScales = Arrays.asList("1961-1990","1971-2000","1981-2010");
		String periods = startDate.replace("-", "") + "-" + endDate.replace("-","");
		try {
			for (String climateScale : climateScales) {
				this.year = year;
				this.climateScale = climateScale;
				List<Map<String, Object>> collect = regionIndexList.stream().filter(
						x->StringUtils.equals(x.get("climateScale").toString(),climateScale)).collect(Collectors.toList());
				packageData(periods,collect);
			}
		} catch (Exception e) {
			return false;
		}
		return true;
	}
	// 区域降水强度数据入库
	public void regionIndexToLibrary(List<Map<String, Object>> regionIndexList) {
		for (Map<String, Object> map : regionIndexList) {
			Map<String, Object> insertMap = new HashMap<>();
			Map<String, Object> delMap = new HashMap<>();
			insertMap.put("year", year);
			insertMap.put("climateScale", climateScale);
			insertMap.put("date", map.get("date"));
			insertMap.put("R0", map.get("R0"));
			
			delMap.put("date", map.get("date"));
			delMap.put("climateScale", climateScale);
			// 数据入库
			CommonHandlerUtils.dataToLibrary(delMap, insertMap, "t_mete_echo_region_psi", year+"伏旱区域降水强度数据入库");
		}
	}
	private void packageData(String periods,List<Map<String, Object>> regionIndexList){
		Map<String, Object> insertMap = new HashMap<>();
		Map<String, Object> delMap = new HashMap<>();
		if (StringUtils.isNotBlank(periods)) {
			String startDate = periods.split("-")[0];
			String endDate = periods.split("-")[1];
			Long lenDays = LocalDateUtils.getDifferDays(startDate, endDate);// 伏旱天数
			// 累计降水强度指数
			double preTotalIndex = regionIndexList.stream().filter(x->StringUtils.compare(x.get("date").toString(),startDate)>=0
					&& StringUtils.compare(x.get("date").toString(),endDate)<0).mapToDouble(x->Double.parseDouble(x.get("R0").toString())).sum();
			insertMap.put("startDate", startDate);
			insertMap.put("endDate", endDate);
			insertMap.put("lenDays", lenDays);
			insertMap.put("preTotalIndex", preTotalIndex);
			// 统计要素特征
			List<Map<String, Object>> basicElements = dao.basicElementCharacteristics(startDate,endDate);
			Map<String, Object> preMap = elementHandler(basicElements,"PRE_Time_2020");
			Map<String, Object> temMap = elementHandler(basicElements,"TEM_Avg");
			Map<String, Object> evpMap = elementHandler(basicElements,"EVP");
			insertMap.put("hingTemDays",preMap.get("hingTemDays"));// 高温日数
			insertMap.put("preLiveVal",preMap.get("liveVal"));// 降水量
			insertMap.put("prePerenVal",preMap.get("perenVal"));// 降水量常年值
			insertMap.put("preAnomalyVal",preMap.get("anomalyVal"));// 降水量距平百分率
			insertMap.put("temLiveVal",temMap.get("liveVal"));// 气温
			insertMap.put("temPerenVal",temMap.get("perenVal"));// 气温常年值
			insertMap.put("temAnomalyVal",temMap.get("anomalyVal"));// 气温距平
			insertMap.put("evpLiveVal",evpMap.get("liveVal"));// 蒸发量
		}
		insertMap.put("year", year);
		insertMap.put("climateScale", climateScale);
		delMap.put("year", year);
		delMap.put("climateScale", climateScale);
		// 数据入库
		CommonHandlerUtils.dataToLibrary(delMap, insertMap, "t_mete_echo_volt_drought", year+"伏旱数据入库");
	}
	/**@Description 对基础要素的数据进行实况和距平的处理等
	 * @param basicElements
	 * @param element
	 * @return
	 * @time 2020年1月6日 上午11:33:34
	 * @author echo
	 */
	private Map<String, Object> elementHandler(List<Map<String, Object>> basicElements,String element) {
		Map<String, Object> resMap = new HashMap<>();
		// 得到当前年份的数据
		List<Map<String, Object>> currentList = basicElements.stream().filter(
				x->StringUtils.equals(x.get("year").toString(), year)).collect(Collectors.toList());
		// 得到常年值区间数据
		List<Map<String, Object>> perenList = basicElements.stream()
				.filter(x -> StringUtils.compare(x.get("year").toString(), climateScale.split("-")[0]) >= 0
						&& StringUtils.compare(x.get("year").toString(), climateScale.split("-")[1]) <= 0)
				.collect(Collectors.toList());
		// 得到站点个数
		long count = currentList.stream().map(x->x.get("stationNO").toString()).distinct().count();
		
		DoubleSummaryStatistics cuSummary = CommonHandlerUtils.filterData(currentList,element).stream()
				.mapToDouble(x->Double.parseDouble(x.get(element).toString())).summaryStatistics();
		DoubleSummaryStatistics perSummary = CommonHandlerUtils.filterData(perenList,element).stream()
				.mapToDouble(x->Double.parseDouble(x.get(element).toString())).summaryStatistics();
		double liveVal = 0d;
		double perenVal = 0d;
		double anomalyVal = 0d;
		if (StringUtils.contains(element, "PRE_Time_2020") || StringUtils.contains(element, "EVP")) {
			liveVal = cuSummary.getSum()/count;
			perenVal = perSummary.getSum()/count/30;
			anomalyVal = DoubleMathUtil.div((liveVal-perenVal)*100, perenVal);
		}else if(StringUtils.contains(element, "TEM_Avg")){
			liveVal = cuSummary.getAverage();
			perenVal = perSummary.getAverage();
			anomalyVal = DoubleMathUtil.sub(liveVal, perenVal);
		}
		// 统计高温日数
		long hingTemDays = currentList.stream().filter(x->Double.parseDouble(
				x.get("TEM_Max").toString())>=35).map(x->x.get("observerTime").toString()).distinct().count();
		resMap.put("liveVal", liveVal);
		resMap.put("perenVal", perenVal);
		resMap.put("anomalyVal", anomalyVal);
		resMap.put("hingTemDays", hingTemDays);
		return resMap;
	}
	/**@Description  得到每一天的区域降水强度指数
	 * @param voltDroughtData
	 * @return
	 * @time 2019年12月27日 下午2:40:53
	 * @author echo
	 */
	private List<Map<String, Object>> regionPrecipitationIndex(List<Map<String, Object>> voltDroughtData) {
		List<Map<String, Object>> resList = new ArrayList<>();
		// 统计年份数据
		List<Map<String, Object>> currentList = voltDroughtData.stream()
				.filter(x -> StringUtils.equals(x.get("year").toString(), year)).collect(Collectors.toList());
		// 得到常年值区间数据
		List<Map<String, Object>> perenList = voltDroughtData.stream()
				.filter(x -> StringUtils.compare(x.get("year").toString(), climateScale.split("-")[0]) >= 0
						&& StringUtils.compare(x.get("year").toString(), climateScale.split("-")[1]) <= 0)
				.collect(Collectors.toList());
		// 得到逐日的时间序列 并从小到大排序
		List<String> dateList = currentList.stream().map(x -> x.get("observerTime").toString()).distinct()
				.sorted((x, y) -> StringUtils.compare(x, y)).collect(Collectors.toList());
		for (String date : dateList) {
			Map<String, Object> map = new HashMap<>();
			// 统计当前时间的集合
			List<Map<String, Object>> cList = currentList.stream()
					.filter(x -> StringUtils.equals(x.get("observerTime").toString(), date))
					.collect(Collectors.toList());
			double qy_Avg = cList.stream().mapToDouble(x -> Double.parseDouble(x.get("PRE_Time_2020").toString()))
					.average().getAsDouble();
			// 统计常年值同一天的数据
			List<Map<String, Object>> pList = perenList.stream()
					.filter(x -> StringUtils.equals(x.get("observerTime").toString().substring(4), date.substring(4)))
					.collect(Collectors.toList());
			double qy_P_Avg = pList.stream().mapToDouble(x -> Double.parseDouble(x.get("PRE_Time_2020").toString()))
					.average().getAsDouble();
			// 得到ΔR 为区域平均日降水量距平百分率
			double ΔR = DoubleMathUtil.div((qy_Avg - qy_P_Avg) * 100, qy_P_Avg);
			double ΔRz = positiveAnomalyRatio(cList, pList);
			double R0 = ΔR + ΔRz;
			map.put("date", date);
			map.put("R0", R0);
			resList.add(map);
		}
		return resList;
	}

	/**@Description 正距平站数占总站数的百分率;
	 * @param cList 实况集合
	 * @param pList 常年集合
	 * @return
	 * @time 2019年12月27日 上午11:17:01
	 * @author echo
	 */
	private double positiveAnomalyRatio(List<Map<String, Object>> cList, List<Map<String, Object>> pList) {
		int allStations = cList.size();
		int positiveStations = 0;
		List<String> staList = cList.stream().map(x -> x.get("stationNo").toString()).distinct()
				.collect(Collectors.toList());
		for (String stationNo : staList) {
			// 统计当前站点的集合
			List<Map<String, Object>> c_List = cList.stream()
					.filter(x -> StringUtils.equals(x.get("stationNo").toString(), stationNo))
					.collect(Collectors.toList());
			double qy_Avg = c_List.stream().mapToDouble(x -> Double.parseDouble(x.get("PRE_Time_2020").toString()))
					.average().getAsDouble();
			// 统计当前常年值同一天的数据
			List<Map<String, Object>> p_List = pList.stream()
					.filter(x -> StringUtils.equals(x.get("stationNo").toString(), stationNo))
					.collect(Collectors.toList());
			double qy_P_Avg = p_List.stream().mapToDouble(x -> Double.parseDouble(x.get("PRE_Time_2020").toString()))
					.average().getAsDouble();
			// 得到ΔR 为区域平均日降水量距平百分率
			if (qy_Avg > qy_P_Avg) {
				positiveStations++;
			}
		}
		double ΔRz = DoubleMathUtil.div(positiveStations*100, allStations);
		return ΔRz;
	}

	/**@Description 得到伏旱的开始和结束时间
	 * @param regionIndexList 区域降水强度指数集合
	 * @return
	 * @time 2019年12月29日 上午11:20:05
	 * @author echo
	 */
	private String droughtPeriod(List<Map<String, Object>> regionIndexList) {
		List<String> startLit =  new ArrayList<>();
		List<String> endLit =  new ArrayList<>();
		// 得到逐日的时间序列  并从小到大排序
		List<String> dateList = regionIndexList.stream().map(x->x.get("date").toString()).distinct()
				.sorted((x,y)->StringUtils.compare(x,y)).collect(Collectors.toList());
		int rSize = dateList.size();
		// 得到伏旱时段所有满足条件的开始时间和结束时间
		for (int i = 0; i < rSize; i++) {
			if (dateCheck(dateList.get(i),regionIndexList)) {// 确定开始时间      
				startLit.add(dateList.get(i));
				i+=8;
			}
		}
		for (int i = 0; i < rSize; i++) {
			if (dateEndCheck(dateList.get(i),regionIndexList)) {// 确定结束时间
				endLit.add(dateList.get(i));
				i+=8;
			}
		}
		// 根据一个开始日期对应一个结束日期,提炼出所有的多雨期时间段。用字符串@分割
			StringJoiner sJoiner = new StringJoiner("@");
			Map<String, String> tempMap = new HashMap<>();
			for (String sDate : startLit) {
				for (String eDate : endLit) {
					String sDate8 = LocalDateUtils.stringToDate(sDate).plusDays(8).toString().replace("-", "");
					if (StringUtils.compare(sDate8, eDate)<0) {
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
		return sJoiner.toString();
	}

	/**
	 * 验证 伏旱开始日8天以上区域降水强度指数小于 0
	 * @param date yyyyMMdd
	 * @param dateList 时间集合
	 * @return
	 */
	private boolean dateCheck(String date, List<Map<String, Object>> regionIndexList) {
		boolean flag = false;
		int count = 0;
		for (int i = 0; i < 8; i++) {
			String tempDate = LocalDateUtils.stringToDate(date).plusDays(i).toString().replace("-", "");
			// 统计当前时间的集合
			Optional<Map<String, Object>> op = regionIndexList.stream()
					.filter(x -> StringUtils.equals(x.get("date").toString(), tempDate)).findFirst();
			if (op.isPresent()) {
				String str = op.get().get("R0").toString();
				if (StringUtils.contains(str, "-")) {
					count++;
				}
			} else {
				break;
			}
			if (count == 8) {
				flag = true;
			}
		}
		return flag;
	}

	/**@Description 伏旱结束日(连续几天(3天之内)降水强度指数之和>=500的第一天的前一天) 的日期)
	 * @param date yyyyMMdd
	 * @param regionIndexList
	 * @return
	 * @time 2019年12月29日 上午11:00:55
	 * @author echo
	 */
	private boolean dateEndCheck(String date, List<Map<String, Object>> regionIndexList) {
		boolean flag = false;
		double sum = 0;
		for (int i = 1; i < 3; i++) {
			String tempDate = LocalDateUtils.stringToDate(date).plusDays(i).toString().replace("-", "");
			// 统计当前时间的集合
			Optional<Map<String, Object>> op = regionIndexList.stream()
					.filter(x -> StringUtils.equals(x.get("date").toString(), tempDate)).findFirst();
			if (op.isPresent()) {
				String str = op.get().get("R0").toString();
				sum += Double.parseDouble(str);
				if (sum >= 500) {
					return true;
				}
			} else {
				break;
			}
		}
		return flag;
	}
	/**@Description 
	 * @param droughtPeriod 伏旱时段
	 * @param regionIndexList
	 * @time 2019年12月30日 上午9:47:29
	 * @author echo
	 */
	private String determineTime(String droughtPeriod,List<Map<String, Object>> regionIndexList) {
		String res = "";
		if(StringUtils.contains(droughtPeriod, "@")) {
			String[] periods = droughtPeriod.split("@");
			res = periods[0];
			for (int i = 1;i<=periods.length-1;i++) {
				res = mergeTimes(res,periods[i],regionIndexList);
			}
		}else {
			return droughtPeriod;
		}
		return res;
	}
	/**@Description 对两个伏旱时间段进行合并操作
	 * @1当相邻两个伏旱时段累计区域降水强度指数和均 ≤- 1000 %(-800%) 
	 * @2且两个伏旱时段之间降水时段正累计区域降水强度指数和 ≤500 % ,则相邻两个伏旱时段可认为是同一伏旱时段。
	 * @param date1 yyyyMMdd-yyyyMMdd
	 * @param date2
	 * @param regionIndexList
	 * @return
	 * @time 2019年12月30日 上午9:54:37
	 * @author echo
	 */
	private String mergeTimes(String date1,String date2,List<Map<String, Object>> regionIndexList) {
		String res = "";
		String[] sp1 = date1.split("-");
		String[] sp2 = date2.split("-");
		if (StringUtils.compare(sp2[0], sp1[1])<=0) {// 如果时间二开始时间比时间一结束时间大  说明时间有重。合并
			res = sp1[0]+"-"+sp2[1];
		}else {
			// 得到两个时间段之间降水强度指数和  以及正累计区域降水强度指数和
			List<Map<String, Object>> collect = regionIndexList.stream().filter(x->StringUtils.compare(x.get("date").toString(),sp1[1])>0
					&& StringUtils.compare(x.get("date").toString(),sp2[0])<0).collect(Collectors.toList());
			double sum1 = collect.stream().mapToDouble(x->Double.parseDouble(x.get("R0").toString())).sum();
			double sum2 = collect.stream().filter(x->Double.parseDouble(x.get("R0").toString())>0)
					.mapToDouble(x->Double.parseDouble(x.get("R0").toString())).sum();
			if(sum1 <= -800 && sum2 <=500) {
				res = sp1[0]+"-"+sp2[1];
			}else {// 不满足条件就取一个时间段长一点的
				Long l1 = LocalDateUtils.getDifferDays(sp1[0], sp1[1]);
				Long l2 = LocalDateUtils.getDifferDays(sp2[0], sp2[1]);
				if (l1 > l2) {
					res = date1;
				}else {
					res = date2;
				}
			}
			
		}
		 return res;
	}
}
