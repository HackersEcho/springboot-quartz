package com.example.demo.dataConversion.climate;

import com.example.demo.utils.CommonHandlerUtils;
import com.example.demo.utils.CommonSqlSelectUtils;
import com.example.demo.utils.LocalDateUtils;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @description:
 * @author: echo
 * @createDate: 2020/3/27
 * @version: 1.0
 */
public class FrostFirstDay {
    public static void main(String[] args) {
        new FrostFirstDay().init();
    }
    public void init(){
        LocalDate nowDate = LocalDate.now();
        int startYears = nowDate.plusYears(-1).getYear();
        int endYears = nowDate.getYear();
        getForstSEDateStatisticsData(startYears, endYears);
    }
    public  void getForstSEDateStatisticsData(int startYears, int endYears) {
        List<Map<String, Object>> staInfos = CommonSqlSelectUtils.getAllStationInfos();
        String strNos = staInfos.stream().map(x -> x.get("stationNo").toString()).distinct().collect(Collectors.joining(","));
        // 首先判断温度是否小于0度，在判断是否在0~2之间
        String conditions1 = "AND  GST_Min>-999 AND GST_Min <= -4";
        String conditions2 = "AND  GST_Min>-999 AND GST_Min <= -2";
        String conditions3 = "AND  GST_Min>-999 AND GST_Min <= 0";
        List<Map<String, Object>> maplistData1 = getForstDate(strNos, conditions1);
        List<Map<String, Object>> maplistData2 = getForstDate(strNos, conditions2);
        List<Map<String, Object>> maplistData3 = getForstDate(strNos, conditions3);
        // 重霜
        String type1 = "重霜";
        // 中霜
        String type2 = "中霜";
        // 轻霜
        String type3 = "轻霜";
        packageData(maplistData1, staInfos, type1, startYears, endYears);
        packageData(maplistData2, staInfos, type2, startYears, endYears);
        packageData(maplistData3, staInfos, type3, startYears, endYears);
    }
    // 将数据封装
    private  void packageData(List<Map<String, Object>> maplistData, List<Map<String, Object>> stationNames,
                              String type, int startYears, int endYears) {
        // 2.确定霜冻初终日等指标，根据年份循环---根据站点循环
        for (int i = 0; i < stationNames.size(); i++) {
            for (int q = startYears; q <= endYears; q++) {
                String stationID = (String) stationNames.get(i).get("stationNo");
                Map<String, Object> infoMap = new HashMap<>();
                int moth = q;
                String startTime = null;
                String endTime = null;
                String dateLength = null;
                String sDateVal = null;
                String eDateVal = null;
                String create_time = LocalDateUtils.DateTimeToString(LocalDateTime.now());
                // 确定一个站当前年份的初日和终日的数据
                List<Map<String, Object>> teMapsDataStart = maplistData.parallelStream()
                        .filter(x -> x.get("stationNo").equals(stationID)
                                && x.get("ObserverTime").toString().startsWith(moth + "")
                                && Integer.parseInt(x.get("ObserverTime").toString().substring(4, 8)) >= 901)
                        .collect(Collectors.toList());
                List<Map<String, Object>> teMapsDataEnd = maplistData.parallelStream()
                        .filter(x -> x.get("stationNo").equals(stationID)
                                && x.get("ObserverTime").toString().startsWith(moth + 1 + "")
                                && Integer.parseInt(x.get("ObserverTime").toString().substring(4, 8)) <= 531)
                        .collect(Collectors.toList());

                // 确定开始时间以及对应的天数和霜冻类型
                if (teMapsDataStart.size() > 0
                        && StringUtils.isNotBlank(teMapsDataStart.get(0).get("ObserverTime").toString())) {
                    startTime = teMapsDataStart.get(0).get("ObserverTime").toString();
                    sDateVal = LocalDateUtils.getNumberOfDays(startTime) + "";
                } else {
                    startTime = "--";
                    sDateVal = "--";
                }
                // 确定结束时间以及对应的天数
                if (teMapsDataEnd.size() > 0
                        && StringUtils.isNotBlank(teMapsDataEnd.get(0).get("ObserverTime").toString())) {
                    endTime = teMapsDataEnd.get(0).get("ObserverTime").toString();
                    eDateVal = LocalDateUtils.getNumberOfDays(endTime) + "";
                } else {
                    endTime = "--";
                    eDateVal = "--";
                }
                // 得到开始和结束的日期长度
                if (StringUtils.equals(startTime, "--") || StringUtils.equals(endTime, "--")) {
                    dateLength = "--";
                } else {
                    dateLength = String.valueOf(LocalDateUtils.getDifferDays(startTime, endTime));
                }
                // 将数据封装到map
                infoMap.put("stationNo", stationID);
                infoMap.put("stationName", stationNames.stream().filter(x -> x.get("stationNo").equals(stationID))
                        .collect(Collectors.toList()).get(0).get("stationName").toString());
                infoMap.put("yearStartDate", startTime);
                infoMap.put("yearEndDate", endTime);
                infoMap.put("yearsDate", q + "");
                infoMap.put("dateLength", dateLength);
                infoMap.put("type", type);
                infoMap.put("sDateVal", sDateVal);
                infoMap.put("eDateVal", eDateVal);
                infoMap.put("create_time", create_time);

                // 将数据插入到数据库
                FrostDatainsert(infoMap);

            }
        }
    }
    // 得到单站霜冻初终日数据
    public  List<Map<String, Object>> getForstDate(String stationNo, String conditions) {
        String sql = "SELECT\n" + "\tstationNo,\n" + "\tDATE_FORMAT(ObserverTime, '%Y%m%d') as ObserverTime,\n"
                + "\tstation_name\n" + "FROM\n" + "\t(\n" + "\t\tSELECT\n" + "\t\t\tt.stationNo,\n"
                + "\t\t\tMIN(t.ObserverTime) AS ObserverTime,\n" + "\t\t\tb.station_name\n" + "\t\tFROM\n"
                + "\t\t\tt_mete_ns_day_data t\n" + "\t\tJOIN t_mete_station b ON t.stationNo = b.device_id\n"
                + "\t\tWHERE\n" + "\t\t\tt.stationNo IN (" + stationNo + ")   " + conditions
                + "\t\tAND DATE_FORMAT(t.ObserverTime, '%m%d') >= '0901'\n"
                + "\t\tAND DATE_FORMAT(t.ObserverTime, '%m%d') <= '1231'\n"
                + "\t\tAND YEAR (ObserverTime) >= 1961\n" + "\t\tGROUP BY\n"
                + "\t\t\tDATE_FORMAT(t.ObserverTime, '%Y'),\n" + "\t\t\tt.stationNo\n" + "UNION\n" + "SELECT\n"
                + "\t\t\tt.stationNo,\n" + "\t\t\tMAX(t.ObserverTime) AS ObserverTime,\n" + "\t\t\tb.station_name\n"
                + "\t\tFROM\n" + "\t\t\tt_mete_ns_day_data t\n"
                + "\t\tJOIN t_mete_station b ON t.stationNo = b.device_id\n" + "\t\tWHERE\n"
                + "\t\t\tt.stationNo IN (" + stationNo + ")   " + conditions
                + "\t\tAND DATE_FORMAT(t.ObserverTime, '%m%d') >= '0101'\n"
                + "\t\tAND DATE_FORMAT(t.ObserverTime, '%m%d') <= '0531'\n"
                + "\t\tAND YEAR (ObserverTime) >= 1961\n" + "\t\tGROUP BY\n"
                + "\t\t\tDATE_FORMAT(t.ObserverTime, '%Y'),\n" + "\t\t\tt.stationNo)tt"
                + "\t\tORDER BY stationNo,ObserverTime";
        return CommonHandlerUtils.sqlHandle(sql, "单站霜冻初终日统计");
    }

    // 霜冻数据插入
    private  void FrostDatainsert(Map<String, Object> maplist) {
        Map<String, Object> delFieldAndVal = new HashMap<String, Object>();
        delFieldAndVal.put("yearsDate", maplist.get("yearsDate"));
        delFieldAndVal.put("type", maplist.get("type"));
        delFieldAndVal.put("stationNo", maplist.get("stationNo"));
        // 数据入库
        CommonHandlerUtils.dataToLibrary(delFieldAndVal,maplist,"t_mete_earlyEnd_copy",maplist.get("stationNo") + "霜冻初终日数据");

    }
}
