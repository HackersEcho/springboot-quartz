package com.example.demo.utils;


import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBUtil {
    private static final Log log =  LogFactory.getLog(DBUtil.class);

    public static Connection getConnection(){
        return getConnection("com.mysql.jdbc.Driver",
                "jdbc:mysql://10.172.14.20:6612/sxcc?serverTimezone=Asia/Shanghai&characterEncoding=utf8&useUnicode=true&useSSL=false",
                "root", "root");
    }
    /**
     * 得到数据库会话对象
     * @param 数据源匹配参数
     */
    public static Connection getConnection(String driverClassName,String url,String username,String password) {
        try {
            Class.forName(driverClassName);
            Connection conn = DriverManager.getConnection(url,username,password);
			log.info("URL:"+url+"的数据库已成功连接");
            return conn;
        } catch (Exception e) {
            log.error("URL:"+url+"的数据库已连接失败",e);
            return null;
        }
    }


    /**
     * 调整表结构
     * @param tableName 表名称
     * @param alertTables 修改表结构的语句
     * @param conn 数据库连接
     * @throws Exception
     */
    public static void alertTable2DB(String tableName,String[] alertTables,Connection conn){
        try{
            conn.setAutoCommit(false);
            PreparedStatement ps = null;
            for(String temp : alertTables){
                ps = conn.prepareStatement(temp);
                ps.executeUpdate();
                ps.close();
            }
            conn.commit();

        }catch(Exception e){
            try {
                conn.rollback();
            } catch (SQLException e1) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 向表中插入数据
     * @param tableName 表名称
     * @param fieldAndVal 字段与值组成的map，其中键为字段值该字段对应的值
     * @param conn 数据库连接
     * @throws Exception 异常处理交由使用它的类
     */
    public static void insertData2DB(String tableName, Map<String,Object> fieldAndVal, Connection conn) throws Exception
    {
        String insertSql = "insert into "+tableName+"(";
        //字段定义
        for(Map.Entry<String,Object> entry : fieldAndVal.entrySet()){
            insertSql = insertSql + entry.getKey()+",";
        }

        insertSql = insertSql.substring(0, insertSql.length()-1)+") values(";
        //字段赋 "?"
        for(Map.Entry<String,Object> entry : fieldAndVal.entrySet()){
            insertSql = insertSql +"?,";
        }
        insertSql = insertSql.substring(0, insertSql.length()-1)+")";
        PreparedStatement ps = conn.prepareStatement(insertSql);
        int i =1;
        for(Map.Entry<String,Object> entry : fieldAndVal.entrySet()){
            ps.setObject(i, entry.getValue());
            i++;
        }
        ps.executeUpdate();
        ps.close();
    }
    /**
     * 向表中插入数据
     * @param tableName 表名称
     * @param fieldAndVal 字段与值组成的map，其中键为字段值该字段对应的值
     * @param conn 数据库连接
     * @throws Exception 异常处理交由使用它的类
     */
    public static void inserBatchtData2DB(String tableName,List<Map<String,Object>> list,Connection conn) throws Exception
    {		Map<String,Object> fieldAndVal = list.get(0);
        Object[][] params = new Object[list.size()][];
        for (int i = 0; i < list.size(); i++) {
            Object[] param= list.get(i).values().toArray();
            params[i] = param;

        }
        String insertSql = "insert into "+tableName+"(";
        //字段定义
        for(Map.Entry<String,Object> entry : fieldAndVal.entrySet()){
            insertSql = insertSql + entry.getKey()+",";
        }

        insertSql = insertSql.substring(0, insertSql.length()-1)+") values(";
        //字段赋 "?"
        for(Map.Entry<String,Object> entry : fieldAndVal.entrySet()){
            insertSql = insertSql +"?,";
        }
        insertSql = insertSql.substring(0, insertSql.length()-1)+")";
        QueryRunner queryRunner = new QueryRunner();
        queryRunner.batch(conn, insertSql, params);
        System.out.println("数据插入成功");

    }

    /**
     * 根据id修改表数据
     * @param tableName 表名称
     * @param id 记录id
     * @param fieldAndVal 字段与值组成的map，其中键为字段值该字段对应的值
     * @param conn 数据库连接
     * @throws Exception 异常处理交由使用它的类
     */
    public static void updateData2DB(String tableName,Integer id,Map<String,Object> fieldAndVal,Connection conn) throws Exception
    {
        String insertSql = "UPDATE "+tableName+" SET ";
        //字段定义
        for(Map.Entry<String,Object> entry : fieldAndVal.entrySet()){
            insertSql = insertSql + entry.getKey()+"="+entry.getValue()+",";
        }
        insertSql = insertSql.substring(0, insertSql.length()-1);
        insertSql = insertSql +" WHERE id ="+id;
        PreparedStatement ps = conn.prepareStatement(insertSql);
        ps.executeUpdate();
        ps.close();
    }

    /**
     * 删除数据库中的数据
     * @param tableName 表名称
     * @param fieldAndVal 字段与值组成的map，其中键为字段值该字段对应的值
     * @param conn 数据库连接
     * @throws Exception 异常处理交由使用它的类
     */
    public static void delBatchData2DB(String tableName,List<Map<String,Object>> list,Connection conn) throws Exception
    {
        Map<String,Object> fieldAndVal = list.get(0);
        Object[][] params = new Object[list.size()][];
        for (int i = 0; i < list.size(); i++) {
            Object[] param= list.get(i).values().toArray();
            params[i] = param;

        }
        String delSql="delete from "+tableName+" where";
        //字段定义
        for(Map.Entry<String,Object> entry : fieldAndVal.entrySet()){
            delSql = delSql + " "+entry.getKey()+"=? and";
        }
        //去掉最后的and
        delSql = delSql.substring(0, delSql.length()-4);
        QueryRunner queryRunner = new QueryRunner();
        int[] nums = queryRunner.batch(delSql, params);
        System.out.println("删除"+nums.length+"条数据");

    }
    /**
     * 批量删除数据库中的数据
     * @param tableName 表名称
     * @param fieldAndVal 字段与值组成的map，其中键为字段值该字段对应的值
     * @param conn 数据库连接
     * @throws Exception 异常处理交由使用它的类
     */
    public static void delData2DB(String tableName,Map<String,Object> fieldAndVal,Connection conn) throws Exception
    {
        String delSql="delete from "+tableName+" where";
        //字段定义
        for(Map.Entry<String,Object> entry : fieldAndVal.entrySet()){
            delSql = delSql + " "+entry.getKey()+"=? and";
        }
        //去掉最后的and
        delSql = delSql.substring(0, delSql.length()-4);

        PreparedStatement ps = conn.prepareStatement(delSql);
        int i =1;
        for(Map.Entry<String,Object> entry : fieldAndVal.entrySet()){
            ps.setObject(i, entry.getValue());
            i++;
        }

        ps.executeUpdate();
        ps.close();
    }

    /**
     * 执行查询sql语句
     * @param sql
     * @param params
     * @param ps
     * @return
     * @throws Exception
     */
    public static ResultSet queryData2DB(Object[] params, PreparedStatement ps) throws Exception
    {
        int i =1;
        if(params != null){
            for(Object param : params)
            {
                ps.setObject(i, param);
                i++;
            }
        }

        return ps.executeQuery();
    }

    /**
     * 查询某个表的字段信息
     * @param conn
     * @param tableName
     * @return
     * @throws Exception
     */
    public static List<Map<String,String>> getColumns(Connection conn, String tableName) throws Exception{
        List<Map<String,String>> results = new ArrayList<Map<String,String>>();
        DatabaseMetaData dbMetaData = conn.getMetaData();
        ResultSet rs = dbMetaData.getColumns(null, null, tableName, "%");
        //ResultSetMetaData rsMetaData = rs.getMetaData(); //列信息查询
        //获取列的所有含有信息
//			for(int i = 0;i < rsMetaData.getColumnCount();i++){
//				System.err.println(rsMetaData.getColumnName(i + 1));
//			}
        while(rs.next()){
            Map<String,String> result = new HashMap<String,String>();
            result.put("COLUMN_NAME",rs.getString("COLUMN_NAME"));
            result.put("TYPE_NAME", rs.getString("TYPE_NAME"));
            result.put("COLUMN_SIZE", rs.getString("COLUMN_SIZE"));
            result.put("DECIMAL_DIGITS",rs.getString("DECIMAL_DIGITS"));
            results.add(result);
        }

        return results;
    }

    /**
     * 返回某个连接所对应库中的所有表名称
     * @param conn
     * @return
     */
    public static List<String> getTables(Connection conn) throws Exception{
        List<String> results = new ArrayList<String>();

        DatabaseMetaData dbMetaData = conn.getMetaData();
        ResultSet rs = dbMetaData.getTables(null, null, "%", new String[] { "TABLE" });
        //ResultSetMetaData rsMetaData = rs.getMetaData(); //表信息查询
        //获取表所的信息
//			for(int i = 0;i < rsMetaData.getColumnCount();i++){
//				System.err.println(rsMetaData.getColumnName(i + 1));
//			}
        while(rs.next()){
            results.add(rs.getString("TABLE_NAME"));
        }

        return results;
    }
}