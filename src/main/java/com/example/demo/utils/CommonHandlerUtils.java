package com.example.demo.utils;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @description:
 * @author: echo
 * @createDate: 2020/3/20
 * @version: 1.0
 */
public class CommonHandlerUtils {
    private static final Log log = LogFactory.getLog(CommonHandlerUtils.class);
    public static List<Map<String, Object>> sqlHandle(String sql, String desc){
        //返回查询结果
        List<Map<String, Object>> queryList = new ArrayList<>();
        QueryRunner queryRunner = new QueryRunner();
        try(Connection conn = DBUtil.getConnection("com.mysql.jdbc.Driver",
                "jdbc:mysql://10.172.14.20:6612/sxcc?serverTimezone=Asia/Shanghai&characterEncoding=utf8&useUnicode=true&useSSL=false",
                "root", "root");) {
            queryList = queryRunner.query(conn, sql, new MapListHandler());
            log.info(desc+"查询成功");
        } catch (Exception e) {
            log.info(desc+"查询失败");
        }

        return queryList;
    }
    /**
     * 将数据入库，已有的删除在入
     * @param delMap 根据主键删除
     * @param insertMap 插入数据map
     * @param tableName 表名
     * @param infos 打印到控制台的信息
     */
    public static void dataToLibrary(Map<String, Object> delMap, Map<String, Object> insertMap, String tableName,
                                     String infos) {
        try(Connection conn = DBUtil.getConnection("com.mysql.jdbc.Driver",
                "jdbc:mysql://10.172.14.20:6612/sxcc?serverTimezone=Asia/Shanghai&characterEncoding=utf8&useUnicode=true&useSSL=false",
                "root", "root");) {
            DBUtil.delData2DB(tableName, delMap, conn);
            DBUtil.insertData2DB(tableName, insertMap, conn);
           log.info(infos+"入库成功");
        } catch (Exception e) {
            log.info(infos+"入库失败");
            e.printStackTrace();
        }
    }
    /**
     * 过滤掉异常数据
     * @param allData
     * @param filed
     * @return
     */
    public static List<Map<String, Object>> filterData(List<Map<String, Object>> data,String filed) {
        return data.stream().filter(x->!Objects.isNull(x.get(filed))
                && Double.parseDouble(x.get(filed).toString())> -999
                && Double.parseDouble(x.get(filed).toString())< 999).collect(Collectors.toList());
    }
    /**
     * 通过站点编号或区域编号得到相应的区域查询条件
     * @param regions 区域编号通过，相连(如果是站点编号也是通过,相连)
     * @author echo
     * @date 2019/04/17
     * @version 1.0
     * @return
     */
    public static String getCondition(String regions) {
        regions = StringUtils.equals(regions, "0")?"1,2,3":regions;//0表示查询全省
        String[] regList = regions.split(",");
        //如果查询是单站或单个区域则用=查询否则用IN
        String condition = "";
        if (regList[0].length() > 3) {// 通过站点查找
            // 对站点编号进行空值处理
            if (StringUtils.isBlank(regions)) {
                regions = "\' \'";
            }
            condition = "b.device_id IN (" + regions + ")";
            if(!StringUtils.contains(regions, ",")) {
                condition = "b.device_id = '" + regions+"'" ;
            }
        } else {// 通过区域编号查找
            String regionField = "";
            if (regList[0].equals("0")||regList[0].equals("1")||regList[0].equals("2")||regList[0].equals("3")) {
                regionField = "region_id_two";
            }else if (regList[0].equals("4")||regList[0].equals("5")) {
                regionField = "basin_id";
            }else {
                regionField = "region_id";
            }
            condition = "b." + regionField +" IN (" + regions + ")";
            if(!StringUtils.contains(regions, ",")) {
                condition = "b." + regionField +"= " + regions;
            }
        }
        return condition;
    }
}
