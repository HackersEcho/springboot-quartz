package com.example.demo.dataConversion.climate;

import com.example.demo.utils.CommonHandlerUtils;
import com.example.demo.utils.DBUtil;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Repository
public class ClimateDao {
	private static final Log log = LogFactory.getLog(ClimateDao.class);
	public List<Map<String, Object>> sqlHandle(String sql,String desc){
		//返回查询结果
				List<Map<String, Object>> queryList = new ArrayList<>();
				QueryRunner queryRunner = new QueryRunner();
				try(Connection conn = DBUtil.getConnection()) {
					queryList = queryRunner.query(conn, sql, new MapListHandler());
				} catch (Exception e) {
				}
				
				return queryList;
	}
	/**
	  *  根据区域类型得到每个区域的站点
	 * @param regionType 站点库中区域类型的字段名称
	 * @author echo
	 * @return
	 */
	public List<Map<String, Object>> findRegionCountsByType(String regionType){
		String sql = "SELECT b."+regionType+" as region FROM t_mete_station b WHERE b.device_id>1 and b."+regionType+">0";
		return sqlHandle(sql, regionType+"区域站点个数");
		
	}
	/**@Description 得到全省自动站点的信息
	 * @return
	 * @time 2019年12月19日 上午10:55:53
	 * @author echo
	 */
	public List<Map<String,Object>> getStationInfos(){
		String sql = "SELECT b.station_name AS stationName,b.device_id AS stationNo FROM t_mete_station b WHERE b.device_id > 5";
		return sqlHandle(sql, "得到自动站的站点信息");
	}

	/**
	 *  得到某一年华西秋雨降雨量(针对降水日表)
	 * @param year 
	 * @param stations 统计的站点数据
	 * @author echo
	 * @return
	 */ 
	public List<Map<String, Object>> getAutumnList(String year,String stations){
		String sql = "SELECT t.stationNo,t.PRE_Time_2020 AS pre,DATE_FORMAT(t.ObserverTime,'%Y%m%d') AS observerTime FROM t_mete_ns_day_data"
				+ " t WHERE DATE_FORMAT(t.ObserverTime,'%Y') = "+year+" AND DATE_FORMAT(t.ObserverTime,'%m%d') \r\n" + 
				"BETWEEN '0821' AND '1031' AND t.stationNo in (" + stations + ") AND t.PRE_Time_2020 BETWEEN 0 AND 999";
		return sqlHandle(sql,year+"华西秋雨");
	}
	/**
	 * 得到77个站点给定时间段的降水
	 * @param startDate
	 * @param endDate
	 * @param stations
	 * @author echo
	 * @return
	 */
	public List<Map<String, Object>> getPrecipitation(String startDate,String endDate,String stations){
		String sql = "SELECT t.stationNo,b.station_name as stationName,b.region_id as region,t.PRE_Time_2020 AS pre,"
					+ "DATE_FORMAT(t.ObserverTime,'%Y%m%d') AS observerTime FROM t_mete_station b JOIN t_mete_ns_day_data "
					+ "t ON t.stationNo = b.device_id WHERE t.ObserverTime BETWEEN '"+startDate
					+"' AND '"+endDate+"' AND t.stationNo in (" + stations + ")"
					+ " AND t.PRE_Time_2020 BETWEEN 0 AND 999";
		return sqlHandle(sql,startDate+"-"+endDate+"降水");
	}
	/**
	 * 从华西秋雨中间表中得到所有的数据
	 * @author echo
	 * @return
	 */
	public List<Map<String, Object>> westRainData(){
		String sql = "SELECT t.year,t.rainyPeriod,DATE_FORMAT(t.startDate,'%Y%m%d') as startDate,"
				+ "DATE_FORMAT(t.endDate,'%Y%m%d') as endDate,t.rainLen,t.rainAmount FROM huaxi_autumn_rain t";
		return sqlHandle(sql,"得到所有华西秋雨表中数据");
	}
	 // 华西秋雨前端表格视图展现数据
	public List<Map<String, Object>> westRainData(String climateScale){
		String sql = "SELECT * FROM inquire_huaxi_rain_table t WHERE t.climateScale = '"+climateScale+"'";
		return sqlHandle(sql,"得到所有华西秋雨表中数据");
	}
	/**
	 * 得到查询时间段内地额降水量
	 * @param startDate yyyyMMdd
	 * @param endDate
	 * @return
	 */
	public List<Map<String, Object>> getAllRainInfo(String startDate,String endDate){
		String sql1 = "SELECT t.stationNo,b.station_name as stationName,b.region_id as region,t.PRE_Time_2020,"
				+ "DATE_FORMAT(t.ObserverTime,'%Y%m%d') AS observerTime\r\n" + 
				"FROM t_mete_station b JOIN t_mete_ns_region_day_data t ON t.stationNo = b.device_id\r\n" + 
				"WHERE t.ObserverTime BETWEEN '"+startDate+"'\r\n" + 
				"AND '"+endDate+"' AND t.PRE_Time_2020 BETWEEN 0 AND 999";
		String sql2 = "SELECT t.stationNo,b.station_name as stationName,b.region_id as region,t.PRE_Time_2020,"
				+ "DATE_FORMAT(t.ObserverTime,'%Y%m%d') AS observerTime FROM t_mete_station b JOIN t_mete_ns_day_data"
				+ " t ON t.stationNo = b.device_id WHERE t.ObserverTime BETWEEN '"+startDate+"'\r\n"  
				+ "AND '"+endDate+"' AND t.PRE_Time_2020 BETWEEN 0 AND 999";
		String sql = sql1 + " UNION "+sql2;
		return sqlHandle(sql,"查询"+startDate+"到"+endDate+"所有站的将水量");
	}
	// ========================霜冻
	/**
	 * 从结果表中获历史取初终日
	 * 
	 * @param regions 区域编号
	 * @param type 霜冻类型
	 * @return
	 */
	public  List<Map<String, Object>> forstFirstDay(String regions,String type) {
		String condition = CommonHandlerUtils.getCondition(regions);//得到区域查询信息
		String sql = "select  stationNo,stationName,yearStartDate,yearEndDate,dateLength,sDateVal,eDateVal,b.latitude AS lat,b.longitude AS lon,"
				+ "yearsDate from  t_mete_earlyEnd_copy t JOIN t_mete_station b ON t.stationNo = b.device_id where "+condition+" and yearsDate>=" + 1961
				+ "  and  type='" + type + "'  order by stationNo,yearsDate";
		
		return sqlHandle(sql,"查询"+regions+"下的历史"+type+"初终日");
	}
	/**
	 * 统计历史同期的指定霜冻类型日数
	 * @param regions 区域编号
	 * @param startDate yyyyMMdd
	 * @param endDate
	 * @param type 霜冻类型
	 * @author echo
	 * @return
	 */
	public  List<Map<String, Object>> forstDaysStatistics(String regions,String startDate,String endDate,String type) {
		String condition = CommonHandlerUtils.getCondition(regions);//得到区域查询信息
		String selCondition = ""; // 根据霜冻类型确定查询条件
		if (StringUtils.equals(type, "重霜")) {
			selCondition =  " GST_Min>-999 AND GST_Min <= -4";
		}else if (StringUtils.equals(type, "中霜")) {
			selCondition =  " GST_Min>-999 AND GST_Min <= -2";
		}else if (StringUtils.equals(type, "轻霜")) {
			selCondition =  " GST_Min>-999 AND GST_Min <= 0";
		}
		// 判断有没有跨年
		String sMD = startDate.substring(4);
		String eMD = endDate.substring(4);
		String sql = "SELECT t.Years as year,t.stationNo,b.station_name AS stationName,b.latitude AS lat,b.longitude AS longitude,DATE_FORMAT(t.ObserverTime,'%Y%m%d') AS observerTime \r\n" + 
				"FROM t_mete_ns_day_data t JOIN t_mete_station b ON t.stationNo = b.device_id\r\n" + 
				"WHERE "+condition+" AND MonthDay BETWEEN '"+sMD+"' AND '"+eMD+"' AND "+selCondition;
		if (StringUtils.compare(sMD, eMD)>0) {
			 sql = "SELECT t.Years as year,t.stationNo,b.station_name AS stationName,b.latitude AS lat,b.longitude AS longitude,DATE_FORMAT(t.ObserverTime,'%Y%m%d') AS observerTime \r\n" + 
					"FROM t_mete_ns_day_data t JOIN t_mete_station b ON t.stationNo = b.device_id\r\n" + 
					"WHERE "+condition+" AND MonthDay BETWEEN '"+sMD+"' AND '"+eMD+"' AND "+selCondition+"\r\n" + 
					"UNION ALL\r\n" + 
					"SELECT (t.Years-1) AS year,t.stationNo,b.station_name AS stationName,b.latitude AS lat,b.longitude AS longitude,DATE_FORMAT(t.ObserverTime,'%Y%m%d') AS observerTime \r\n" + 
					"FROM t_mete_ns_day_data t JOIN t_mete_station b ON t.stationNo = b.device_id\r\n" + 
					"WHERE "+condition+" AND MonthDay BETWEEN '"+sMD+"' AND '"+eMD+"' AND "+selCondition;
		}
		
		return sqlHandle(sql,"查询"+sMD+"~"+eMD+type+"同期日数统计");
	}
	// =============================== 绘图数据
	public List<Map<String, Object>> drawPhenomenonDays(String regionId, String startDate, String endDate, String code) {
		String condition = CommonHandlerUtils.getCondition(regionId);//得到区域查询信息
		String sql = "SELECT t.stationNo,DATE_FORMAT(t.ObserverTime,'%Y%m%d') AS observerTime,b.latitude AS lat,\r\n" + 
				"b.longitude AS lon FROM t_mete_station b JOIN t_mete_ns_day_wea t ON t.stationNo = b.device_id\r\n" + 
				"WHERE "+condition+" AND t.ObserverTime BETWEEN '"+startDate+"' AND '"+endDate+"' AND t.WEP_Code = '"+code+"' ";
		return sqlHandle(sql,"查询"+startDate+"~"+endDate+"天气现象码为"+code+"日数统计数据");
	}
	/**
	 * 全省无霜日数统计
	 * @param startDate
	 * @param endDate
	 * @param gstMin
	 * @return
	 */
	public List<Map<String, Object>> getDayCountByRegion(String startDate, String endDate, String gstMin) {
		String sql = "select  a.region_id_two AS regid,t.ObserverTime AS time  from t_mete_ns_day_data t \n"
				+ ",t_mete_station a" + " where t.stationNo = a.device_id and t.ObserverTime>='" + startDate + "' \n"
				+ "and t.ObserverTime<='" + endDate + "'  and a.station_type =1  " + "and t.GST_Min>" + gstMin;
		return sqlHandle(sql,"统计无霜日数");
	}
	//============ 四季入季数据查询
	/**@Description 查询指定年份的所有平均气温数据
	 * @param regions
	 * @param startYear
	 * @param endYear
	 * @return
	 * @time 2019年12月19日 上午10:46:54
	 * @author echo
	 */
	public List<Map<String, Object>> getSeasonData(String regions,String startYear, String endYear) {
		String condition = CommonHandlerUtils.getCondition(regions);//得到区域查询信息
		String sql = "SELECT t.stationNo,b.station_name AS stationName,DATE_FORMAT(t.ObserverTime, '%Y%m%d') AS observerTime,\r\n" + 
				"t.TEM_Avg,t.Years,t.MonthDay FROM t_mete_ns_day_data t JOIN t_mete_station b ON t.stationNo = b.device_id\r\n" + 
				"WHERE "+condition+" AND Years BETWEEN "+startYear+" AND "+endYear+" AND t.TEM_Avg BETWEEN - 99 AND 99";
		return sqlHandle(sql,startYear+"-"+endYear+"平均气温");
	}
	// ===============伏旱
	/**@Description 
	 * @return
	 * @time 2019年12月27日 上午10:33:11
	 * @author echo
	 */
	public List<Map<String, Object>> getVoltDroughtData() {
		String sql = "SELECT t.stationNo,b.station_name,t.PRE_Time_2020,DATE_FORMAT(t.ObserverTime,'%Y%m%d') AS observerTime,"
				+ " t.Years as year FROM t_mete_ns_day_data t JOIN t_mete_station b ON t.stationNo = b.device_id \r\n" 
//				+ " WHERE  b.region_id_two IN (2,3) AND b.device_id != 53945 AND "
				+ " WHERE  "
				+ " t.MonthDay BETWEEN 701 AND 831 AND t.PRE_Time_2020 BETWEEN 0 AND 999 "
				+ " AND b.station_name IN ('宝鸡','陇县','彬县','武功','周至','泾河','渭南','大荔','韩城','略阳','汉中','西乡','石泉','安康','周州')  ";
		return sqlHandle(sql,"历年七八月份的降水量/伏旱数据统计");
	}
	/**    
	 * @Description: TODO(得到同期所有伏旱需要统计的要素)   
	 * @param startDate
	 * @param endDate
	 * @author: echo    
	 * @date:   2019年12月31日 下午4:25:27      
	 * @return: List<Map<String,Object>>      
	 */ 
	public List<Map<String, Object>> basicElementCharacteristics(String startDate, String endDate) {
		int st = Integer.parseInt(startDate.replace("-", "").substring(4));
		int et = Integer.parseInt(endDate.replace("-", "").substring(4));
		String sql = "SELECT t.stationNo,DATE_FORMAT(t.ObserverTime,'%Y%m%d') AS observerTime,t.Years AS year,t.PRE_Time_2020,t.TEM_Avg,t.TEM_Max,t.EVP\r\n" + 
				" FROM t_mete_ns_day_data t WHERE t.MonthDay BETWEEN "+st+" AND "+et;
		return sqlHandle(sql,"同期所有伏旱需要统计的要素");
	}
	// 监测系统降水强度指数表格
	public List<Map<String, Object>> getVoltDroughtPSIData(String startDate,String endDate,String climateScale,
			String min,String max) {
		String sql = "SELECT * FROM t_mete_echo_region_psi WHERE date BETWEEN '"+startDate+"' AND '"+endDate+"'\r\n" + 
				"AND climateScale = '"+climateScale+"' AND R0 BETWEEN "+min+" AND "+ max;
		return sqlHandle(sql,"降水强度指数表格");
	}
	public List<Map<String, Object>> getVoltDroughtPSIData(String year) {
		String sql = "SELECT * FROM t_mete_echo_region_psi WHERE year = "+year;
		return sqlHandle(sql,"降水强度指数表格");
	}
	// 监测伏旱过程统计
	public List<Map<String, Object>> getDroughtProcess(String climateScale) {
		String sql = "SELECT * FROM t_mete_echo_volt_drought WHERE climateScale = '"+climateScale+"'";
		return sqlHandle(sql,"监测伏旱过程统计");
	}
}
