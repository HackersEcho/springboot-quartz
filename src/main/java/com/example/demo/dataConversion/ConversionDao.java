package com.example.demo.dataConversion;

import com.example.demo.utils.DBUtil;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Repository
public class ConversionDao {
	private static final Log log = LogFactory.getLog(ConversionDao.class);
	
	private List<Map<String, Object>> sqlHandle(String sql,String desc){
		//返回查询结果
				List<Map<String, Object>> queryList = new ArrayList<>();
				QueryRunner queryRunner = new QueryRunner();
				try(Connection conn = DBUtil.getConnection();) {
					queryList = queryRunner.query(conn, sql, new MapListHandler());
				} catch (Exception e) {
				}
				
				return queryList;
	}
	/**@Description 得到日数据查询时间段的所有数据
	 * @param startDate yyyyMMdd
	 * @param endDate
	 * @return
	 * @time 2019年12月23日 下午3:09:38
	 * @author echo
	 */
	public List<Map<String, Object>> basicDataQuery(String startDate,String endDate) {
		String sql = "SELECT * FROM t_mete_ns_day_data t "
				+ "WHERE t.ObserverTime BETWEEN '"+startDate+"' AND '"+endDate+"'";
		return sqlHandle(sql, startDate+"~"+endDate+"日表数据查询");
	}
	
}
