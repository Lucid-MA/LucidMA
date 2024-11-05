package com.jvulopas.cash_tracker;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author jvulopas
 * Model connection to SQL database.
 */
public class HelixConnection {
	private final Connection conn;
	private final Statement statement;
	
	public HelixConnection() throws SQLException {
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String url =    "jdbc:sqlserver://LUCIDSQL1;"
		                + "database=HELIXREPO_PROD_02;"
		                + "user=mattiasalmers;"
		                + "password=12345;";
//		                + "encrypt=true;"
//		                + "trustServerCertificate=false;"
//		                + "loginTimeout=30;";
		
		conn = DriverManager.getConnection(url);
		statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
	}
	
	/**
	 * Perform SQL query
	 * @param query query text to execute
	 * @return query results
	 * @throws SQLException
	 */
	public ResultSet query(String query) throws SQLException {
		return statement.executeQuery(query);
	}
	
	public void close() throws SQLException {
		statement.close();
		conn.close();
	}
	
	/**
	 * Query for repos and reverses on date.
	 * @param valDate
	 * @return
	 */
	public static final String TRADE_QUERY_ON_DATE(Date valDate) {
		String pattern = "yyyy-MM-dd";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern); // "set @valdate = '" + simpleDateFormat.format(valDate) + "'\r\n"
		return  "declare @valdate date\r\n" + 
				"set @valdate = '" + simpleDateFormat.format(valDate) + "'\r\n" + 
				"\r\n" + 
				"select \r\n" + 
				"concat((case when tradepieces.company in(44,46) then tradepieces.tradepiece when ltrim(rtrim(tradepieces.ledgername)) = 'Master' and (tradepieces.company = 45) then Tradepieces.TRADEPIECE\r\n" + 
				"else tradepiecexrefs.frontofficeid\r\n" + 
				"end), ' ', (case when tradepieces.startdate = @valdate then 'TRANSMITTED' else 'CLOSED' end)) action_id,\r\n" + 
				"case when tradepieces.company = 44 then 'USG' when tradepieces.company = 45 then 'PRIME' when tradepieces.company = 46 then 'MMT' end fund, \r\n" + 
				"upper(ltrim(rtrim(ledgername))) series,\r\n" + 
				"\r\n" + 
				"\r\n" + 
				"/* crucial column. if only one series in fund, this should be true, else false */\r\n" + 
				"case when not tradepieces.company = 45 then 1 else 0 end is_also_master,\r\n" + 
				"case when tradepiecexrefs.frontofficeid <> 0 then tradepieces.par * 1.0 /masterpieces.masterpar else 1 end used_alloc,\r\n" + 
				"tradepieces.tradetype trade_type,\r\n" + 
				"tradepieces.startdate start_date, CASE WHEN tradepieces.closedate is null then tradepieces.enddate else tradepieces.closedate END as end_date,\r\n" + 
				"case when tradepieces.enddate = @valdate then 1 else 0 end set_to_term_on_date,\r\n" + 
				"tradepieces.cusip security,\r\n" + 
				"tradepieces.isgscc is_buy_sell,\r\n" + 
				"tradepieces.par quantity,\r\n" + 
				"tradepieces.money,\r\n" + 
				"(Tradepieces.money + TRADEPIECECALCDATAS.REPOINTEREST_UNREALIZED + TRADEPIECECALCDATAS.REPOINTEREST_NBD) end_money, \r\n" + 
				"case when (tradepieces.company = 45 and ltrim(rtrim(tradepieces.ledgername)) = 'Master') or tradepieces.company in (44,46) then tradepiecexrefs.frontofficeid else '' end roll_of,\r\n" + 
				"case when ltrim(rtrim(Tradepieces.acct_number)) = '400CAPTX' then 'TEX' ELSE ltrim(rtrim(Tradepieces.acct_number)) END counterparty,\r\n" + 
				"Tradepieces.depository\r\n" + 
				"from tradepieces join TRADEPIECECALCDATAS on tradepieces.tradepiece = TRADEPIECECALCDATAS.tradepiece\r\n" + 
				"join TRADECOMMISSIONPIECEINFO on tradepieces.tradepiece = TRADECOMMISSIONPIECEINFO.TRADEPIECE JOIN TRADEPIECEXREFS ON TRADEPIECES.TRADEPIECE=TRADEPIECEXREFS.TRADEPIECE\r\n" + 
				"left join (select tradepiece masterpiece, par masterpar from tradepieces) masterpieces on\r\n" + 
				"tradepiecexrefs.frontofficeid = masterpieces.masterpiece\r\n" + 
				"where (Tradepieces.startdate = @valdate or CASE WHEN tradepieces.closedate is null then tradepieces.enddate else tradepieces.closedate END = @valdate)\r\n" + 
				"and tradepieces.company in (44,45)\r\n" + 
				"and tradepieces.statusmain not in (6) /* TODO should exclude this for historical?*/\r\n" + 
				"and Tradepieces.tradetype in (0, 1) /* 0 - repo ; 1 - reverse */\r\n" + 
				"and Tradepieces.tradepiece not in (37090,37089,37088,37087,37086,37085,37084,37083,37082,37081) "+
				"order by tradepieces.company, action_id, case when upper(ltrim(rtrim(tradepieces.ledgername))) = 'MASTER' then 0 else 1 end";
	}
	
	/**
	 * Query for repo and reverse free trades (margin trades and others) on date.
	 * @param valDate
	 * @return
	 */
	public static final String TRADESFREE_QUERY_ON_DATE(Date valDate) {
		String pattern = "yyyy-MM-dd";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern); // "set @valdate = '" + simpleDateFormat.format(valDate) + "'\r\n"
		return  "declare @valdate date\r\n" + 
				"set @valdate = '" + simpleDateFormat.format(valDate) + "'\r\n" + 
				"\r\n" + 
				"/* repo frees and reverse frees */\r\n" + 
				"select \r\n" + 
				"concat((case when tradepieces.company in(44,46) then tradepieces.tradepiece when ltrim(rtrim(tradepieces.ledgername)) = 'Master' and (tradepieces.company = 45) then Tradepieces.TRADEPIECE\r\n" + 
				"else tradepiecexrefs.frontofficeid\r\n" + 
				"end), ' ', (case when tradepieces.startdate = @valdate then 'TRANSMITTED' else 'CLOSED' end)) action_id,\r\n" + 
				"case when tradepieces.company = 44 then 'USG' when tradepieces.company = 45 then 'PRIME' when tradepieces.company = 46 then 'MMT' end fund, \r\n" + 
				"upper(ltrim(rtrim(ledgername))) series,\r\n" + 
				"case when tradepiecexrefs.frontofficeid <> 0 then tradepieces.par* 1.0/masterpieces.masterpar else 1 end used_alloc,\r\n" + 
				"\r\n" + 
				"/* crucial column. if only one series in fund, this should be true, else false */\r\n" + 
				"case when not tradepieces.company = 45 then 1 else 0 end is_also_master,\r\n" + 
				"\r\n" + 
				"tradepieces.startdate start_date, tradepieces.closedate close_date, tradepieces.enddate end_date,\r\n" + 
				"par * case when (tradepieces.tradetype = 23 and tradepieces.startdate = @valdate) or (tradepieces.tradetype = 22 and (tradepieces.CLOSEDATE = @valdate or tradepieces.enddate = @valdate)) then 1 \r\n" + 
				"				when (tradepieces.tradetype = 22 and tradepieces.startdate = @valdate) or (tradepieces.tradetype = 23 and (tradepieces.CLOSEDATE = @valdate or tradepieces.enddate = @valdate)) then -1 \r\n" + 
				"				else 0 end \"amount\",\r\n" + 
				"tradepieces.tradetype trade_type,\r\n" + 
				"tradepieces.cusip \"security\",\r\n" + 
				"case when ltrim(rtrim(Tradepieces.acct_number)) = '400CAPTX' then 'TEX' ELSE ltrim(rtrim(Tradepieces.acct_number)) END \"counterparty\", \r\n" + 
				"concat(case when (tradepieces.tradetype = 23 and tradepieces.startdate = @valdate) then 'Receive '\r\n" + 
				"	 when (tradepieces.tradetype = 22 and tradepieces.startdate = @valdate) then 'Pay '\r\n" + 
				"	 when (tradepieces.tradetype = 23 and (tradepieces.CLOSEDATE = @valdate or tradepieces.enddate = @valdate)) then 'Return '\r\n" + 
				"	 when (tradepieces.tradetype = 22 and (tradepieces.CLOSEDATE = @valdate or tradepieces.enddate = @valdate)) then 'Receive returned '\r\n" + 
				"end, case when ltrim(rtrim(Tradepieces.acct_number)) = '400CAPTX' then 'TEX' ELSE ltrim(rtrim(Tradepieces.acct_number)) END, ' margin') \"description\"\r\n" + 
				"from tradepieces join TRADEPIECECALCDATAS on tradepieces.tradepiece = TRADEPIECECALCDATAS.tradepiece\r\n" + 
				"join TRADECOMMISSIONPIECEINFO on tradepieces.tradepiece = TRADECOMMISSIONPIECEINFO.TRADEPIECE JOIN TRADEPIECEXREFS ON TRADEPIECES.TRADEPIECE=TRADEPIECEXREFS.TRADEPIECE\r\n" + 
				"left join (select tradepiece masterpiece, par masterpar from tradepieces) masterpieces on\r\n" + 
				"tradepiecexrefs.frontofficeid = masterpieces.masterpiece\r\n" + 
				"where (Tradepieces.startdate = @valdate or Tradepieces.enddate = @valdate or Tradepieces.closedate = @valdate)\r\n" + 
				"and tradepieces.company in (44,45)\r\n" +
				"and Tradepieces.tradetype in (22,23)\r\n" + 
				"and tradepieces.statusmain not in (6)\r\n" + 
				"order by tradepieces.company, case when upper(ltrim(rtrim(tradepieces.ledgername))) = 'MASTER' then 0 else 1 end";
	}
	
	/**
	 * Query for net margin cash balance by counterparty
	 * @param valDate
	 * @return
	 */
	public static final String NET_CASH_BY_COUNTERPARTY(Date valDate) {
		String pattern = "yyyy-MM-dd";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
		return "declare @valdate date\r\n" + 
				"set @valdate = '" + simpleDateFormat.format(valDate) + "'\r\n" + 
				"select tbl1.fund, case when ltrim(rtrim(TBL1.acct_number)) = '400CAPTX' then 'TEX' ELSE ltrim(rtrim(TBL1.acct_number)) END acct_number, ltrim(rtrim(tbl1.ledgername)) ledgername, tbl1.net_cash, case when tbl2.activity is null then 0 else tbl2.activity end activity, tbl1.is_also_master from \r\n" + 
				"(select case when company = 44 then 'USG' when company = 45 then 'PRIME' when tradepieces.company = 46 then 'MMT' else 'Other' end fund,case when ltrim(rtrim(acct_number)) = '400CAPTX' then 'TEX' ELSE ltrim(rtrim(acct_number)) END acct_number, ltrim(rtrim(ledgername)) ledgername, round(sum(\r\n" + 
				"case when tradetype = 22 then -1 else 1 end * case when (tradepieces.closedate = @valdate or tradepieces.enddate = @valdate) then 0 else 1 end * par),2) 'net_cash', case when not company = 45 then 1 else 0 end is_also_master from tradepieces\r\n" + 
				"where (tradepieces.startdate <= @valdate and (tradepieces.closedate >= @valdate or ((tradepieces.enddate is null or tradepieces.enddate >= @valdate) and tradepieces.closedate is null))) and company in (44,45) and tradetype in (22,23) and cusip = 'CASHUSD01'\r\n" + 
				"and statusmain not in (6)\r\n" + 
				"group by company, ledgername,case when ltrim(rtrim(acct_number)) = '400CAPTX' then 'TEX' ELSE ltrim(rtrim(acct_number)) END) tbl1 full outer join\r\n" + 
				"(select case when company = 44 then 'USG' when company = 45 then 'PRIME' when tradepieces.company = 46 then 'MMT' else 'Other' end fund,case when ltrim(rtrim(acct_number)) = '400CAPTX' then 'TEX' ELSE ltrim(rtrim(acct_number)) END acct_number, ltrim(rtrim(ledgername)) ledgername, round(sum(\r\n" + 
				"case when tradetype = 22 then -1 else 1 end * case when startdate = @valdate then 1 else -1 end * par),2) 'activity', case when not company = 45 then 1 else 0 end is_also_master from tradepieces\r\n" + 
				"where (tradepieces.startdate = @valdate or tradepieces.closedate = @valdate or (tradepieces.enddate = @valdate and tradepieces.closedate is null)) \r\n" + 
				"and company in (44,45) and tradetype in (22,23) and cusip = 'CASHUSD01'\r\n" + 
				"and statusmain not in (6)\r\n" + 
				"group by company, ltrim(rtrim(ledgername)),case when ltrim(rtrim(acct_number)) = '400CAPTX' then 'TEX' ELSE ltrim(rtrim(acct_number)) END) tbl2 on\r\n" + 
				"tbl1.fund = tbl2.fund and\r\n" + 
				"case when ltrim(rtrim(TBL1.acct_number)) = '400CAPTX' then 'TEX' ELSE ltrim(rtrim(TBL1.acct_number)) END = case when ltrim(rtrim(TBL2.acct_number)) = '400CAPTX' then 'TEX' ELSE ltrim(rtrim(TBL2.acct_number)) END and\r\n" + 
				"ltrim(rtrim(tbl1.ledgername)) = ltrim(rtrim(tbl2.ledgername))\r\n" + 
				"order by tbl1.fund, case when upper(ltrim(rtrim(tbl1.ledgername))) = 'MASTER' then 0 else 1 end\r\n";
	}
	
	
	
	public static void main(String[] args) {
//		HelixConnection conn = null;
//		try {
//			conn = new HelixConnection();
//			ResultSet rs = conn.query(HelixConnection.TRADE_QUERY_ON_DATE(new Date()));
//			System.out.println(rs.toString());
//		} catch (SQLException e) {
//			e.printStackTrace();
//		} finally {
//			try {
//				conn.close();
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
		System.out.println("***************************Trade query");
		System.out.println(HelixConnection.TRADE_QUERY_ON_DATE(new Date()));
		System.out.println("*************************Trade margin query");
		System.out.println(HelixConnection.TRADESFREE_QUERY_ON_DATE(new Date()));
		System.out.println("*************************Net cash query");
		System.out.println(HelixConnection.NET_CASH_BY_COUNTERPARTY(new Date()));
	}

}
