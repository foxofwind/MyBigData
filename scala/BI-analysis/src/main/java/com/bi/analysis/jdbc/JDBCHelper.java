package com.bi.analysis.jdbc;

import com.bi.analysis.util.DBPoolConnection;

import java.sql.*;
import java.util.List;

/**
 * JDBC辅助组件
 * @author Administrator
 *
 */
public class JDBCHelper {

	private static JDBCHelper instance = null;

	/**
	 * 获取单例
	 * @return 单例
	 */
	public static JDBCHelper getInstance() {
		if(instance == null) {
			synchronized(JDBCHelper.class) {
				if(instance == null) {
					instance = new JDBCHelper();
				}
			}
		}
		return instance;
	}

	private JDBCHelper() {}

	/**
	 * 执行增删改SQL语句
	 * @param sql
	 * @param params
	 * @return 影响的行数
	 */
	public int executeUpdate(String sql, Object[] params) {
		int rtn = 0;
		Connection conn = null;
		PreparedStatement pstmt = null;
		DBPoolConnection dbPoolConnection = DBPoolConnection.getInstance();
		try{
			conn = dbPoolConnection.getConnection();
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement(sql);
			if(params != null && params.length > 0) {
				for(int i = 0; i < params.length; i++) {
					pstmt.setObject(i + 1, params[i]);
				}
			}
			rtn = pstmt.executeUpdate();
			conn.commit();
			pstmt.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			if(pstmt != null){
				try {
					pstmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}

		return rtn;
	}

	/**
	 * 执行查询SQL语句
	 * @param sql
	 * @param params
	 * @param callback
	 */
	public void executeQuery(String sql, Object[] params,
							 QueryCallback callback) {
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		DBPoolConnection dbPoolConnection = DBPoolConnection.getInstance();
		try{
			conn = dbPoolConnection.getConnection();
			pstmt =  conn.prepareStatement(sql);
			if(params != null && params.length > 0) {
				for(int i = 0; i < params.length; i++) {
					pstmt.setObject(i + 1, params[i]);
				}
			}
			rs = pstmt.executeQuery();
			callback.process(rs);

			pstmt.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			if(pstmt != null){
				try {
					pstmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 批量执行SQL语句
	 * @param sql
	 * @param paramsList
	 * @return 每条SQL语句影响的行数
	 */
	public int[] executeBatch(String sql, List<Object[]> paramsList) {
		int[] rtn = null;
		Connection conn = null;
		PreparedStatement pstmt = null;
		DBPoolConnection dbPoolConnection = DBPoolConnection.getInstance();

		try{
			conn = dbPoolConnection.getConnection();
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement(sql);
			if(paramsList != null && paramsList.size() > 0) {
				for(Object[] params : paramsList) {
					for(int i = 0; i < params.length; i++) {
						pstmt.setObject(i + 1, params[i]);
					}
					pstmt.addBatch();
				}
			}
			rtn = pstmt.executeBatch();
			conn.commit();
			pstmt.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(pstmt != null){
				try {
					pstmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}

		}

		return rtn;
	}

	/**
	 * 静态内部类：查询回调接口
	 * @author Administrator
	 *
	 */
	public static interface QueryCallback {

		/**
		 * 处理查询结果
		 * @param rs
		 * @throws Exception
		 */void process(ResultSet rs) throws Exception;

	}

}
