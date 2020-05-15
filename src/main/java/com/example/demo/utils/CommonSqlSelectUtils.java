package com.example.demo.utils;

import java.util.List;
import java.util.Map;

/**
 * @description:
 * @author: echo
 * @createDate: 2020/3/27
 * @version: 1.0
 */
public class CommonSqlSelectUtils {
    // 得到所有自动站的站点信息
    public static List<Map<String, Object>> getAllStationInfos(){
        String sql = "SELECT b.station_name AS stationName , b.device_id AS stationNo,b.longitude AS lon," +
                "b.latitude AS lat FROM t_mete_station b WHERE b.station_type = 1";
        return CommonHandlerUtils.sqlHandle(sql, "全省自动站点信息查询");
    }
}
