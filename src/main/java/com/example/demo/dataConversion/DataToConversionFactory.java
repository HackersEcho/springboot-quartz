package com.example.demo.dataConversion;

import com.example.demo.utils.DBUtil;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.time.LocalDate;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
/**
 * 将日数据转化为侯 月 季 年的通用模板
 * @author Administrator
 *
 */
public abstract class DataToConversionFactory {
	private static final Double DEFAULTVLUES = 999999d;
	private static ConversionDao dao = new ConversionDao();
	protected static String tableName;// 需要实现的子类在同步的时候去实现
	protected static String dataType;// 数据的类型  如：月数据
	
	protected abstract void historySync();// 同步历史数据
	protected abstract void realTimeSync();// 同步实时数据
	
	public void init(String startDate,String endDate) {
		List<Map<String, Object>> basicData = dao.basicDataQuery(startDate, endDate);
		List<String> staList = basicData.stream().map(
				x->x.get("stationNo").toString()).distinct().collect(Collectors.toList());
		for (String stationNo : staList) {
			Map<String, Object> insertMap = new HashMap<>();
			Map<String, Object> delMap = new HashMap<>();
			List<Map<String, Object>> singleList = basicData.stream().filter(x->StringUtils.equals(
					stationNo, x.get("stationNo").toString())).collect(Collectors.toList());
			String[] elementList = { "TEM_Avg", "TEM_Max", "TEM_Min", "PRE_Time_2020", "PRE_Time_2008", "PRE_Time_0820",
					"PRE_Time_2008", "PRE_Time_0808", "SSH", "EVP", "EVP_Big", "GST_Avg", "GST_Max", "GST_Min",
					"GST_Avg_5cm", "GST_Avg_10cm", "GST_Avg_15cm", "GST_Avg_20cm", "PRS_Avg", "PRS_Max", "PRS_Min",
					"RHU_Avg", "RHU_Min", "Snow_Depth", "VAP_Avg", "VIS_Min", "WIN_S_Max", "WIN_S_Inst_Max" };
			for (String element : elementList) {
				double val = dataStrategy.apply(singleList, element);
				insertMap.put(element, val);
				if((StringUtils.containsIgnoreCase(element, "min") || 
						StringUtils.containsIgnoreCase(element, "max")) && val!=DEFAULTVLUES) {
					// 得到最大或者最小元素对应的时间
					Optional<Map<String, Object>> op = singleList.parallelStream()
							.filter(x -> x.get(element)!= null && Double.parseDouble(x.get(element).toString()) == val).findFirst();
					if (op.isPresent()) {
						insertMap.put(element + "_OTime", op.get().get("ObserverTime"));
					}
				}
			}
			insertMap.put("stationNo", stationNo);
			insertMap.put("ObserverTime", startDate);
			insertMap.put("insertTime", LocalDate.now().toString());// 插入时间
			
			delMap.put("stationNo", stationNo);
			delMap.put("ObserverTime", startDate);
			
			insertData(delMap,insertMap);
			
		}
	}
	/**
	 * 根据不同的字段进行对应的求和或平均   
	 * 		如：降水和日照求和   最低气温求最小值
	 * @author echo
	 * @date 2019/11/29
	 */
		BiFunction<List<Map<String, Object>>, String, Double> dataStrategy = (data,filed)->{
			List<Map<String, Object>> filterData = filterData(data,filed);// 过滤掉异常数据
			double resVal = 0d;
			if (filterData.size()>0) {
				DoubleSummaryStatistics summaryStatistics = filterData.stream()
						.mapToDouble(x->Double.parseDouble(x.get(filed).toString())).summaryStatistics();
				if (StringUtils.containsIgnoreCase(filed, "AVG")) {
					resVal = summaryStatistics.getAverage();
				}else if (StringUtils.containsIgnoreCase(filed, "min")) {
					resVal = summaryStatistics.getMin();
				}else if (StringUtils.containsIgnoreCase(filed, "max")) {   
					resVal = summaryStatistics.getMax();
				}else {// 其他的字段都统一求和
					resVal = summaryStatistics.getSum();
				}
				return resVal;
			}
			return DEFAULTVLUES;// 如果处理的数据为空  那么填充为默认值
		};
		private void insertData(Map<String, Object> delMap,Map<String, Object> insertMap) {
			try(Connection conn = DBUtil.getConnection()) {
				DBUtil.delData2DB(tableName, delMap, conn);
				DBUtil.insertData2DB(tableName, insertMap, conn);
				System.out.println(insertMap.get("stationNo")+"-"+insertMap.get("ObserverTime") + dataType+"成功");
			} catch (Exception e) {
				System.out.println(insertMap.get("stationNo") + dataType+"失败");
				e.printStackTrace();
			}
		}
		/**
		 * 过滤掉异常数据
		 * @param allData
		 * @param filed
		 * @return
		 */
		private List<Map<String, Object>> filterData(List<Map<String, Object>> data,String filed) {
			return data.stream().filter(x->!Objects.isNull(x.get(filed))
					&& Double.parseDouble(x.get(filed).toString())> -999
					&& Double.parseDouble(x.get(filed).toString())< 999).collect(Collectors.toList());
		}
}
