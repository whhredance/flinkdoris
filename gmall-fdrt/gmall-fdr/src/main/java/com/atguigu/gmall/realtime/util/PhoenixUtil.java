package com.atguigu.gmall.realtime.util;

import java.sql.*;

public class PhoenixUtil {
    /**
     * 用于执行 Phoenix 建表语句或插入语句
     * @param sql 待执行的语句
     * @param conn Phoenix 连接对象
     */
    public static void executeSQL(String sql, Connection conn) {
        PreparedStatement ps = null;
        try {
            //获取数据库操作对象
            ps = conn.prepareStatement(sql);
            //执行SQL语句
            ps.execute();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Phoenix 建表语句或插入语句执行异常");
        } finally {
            close(ps, conn);
        }
    }

    /**
     * 用于释放资源
     * @param ps 数据库操作对象
     * @param conn 连接对象
     */
    public static void close(PreparedStatement ps, Connection conn) {
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
