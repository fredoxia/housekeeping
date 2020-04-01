package service;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

public class CleanVIPScore extends AbstractArchiving implements Runnable{
	public CleanVIPScore(String confFile) {
		super(confFile);
		// TODO Auto-generated constructor stub
	}

	private Set<Integer> chainIds = new HashSet<Integer>();

	private String date1 = "2020-3-1 00:00:00";
	private String date2 = "2020-4-1 00:00:00";
	private String logFileName = "";
	
	public String getLogFileName() {
		return logFileName;
	}

	public void setLogFileName(String logFileName) {
		this.logFileName = logFileName;
	}

	public Set<Integer> getChainIds() {
		return chainIds;
	}

	public void setChainIds(Set<Integer> chainIds) {
		this.chainIds = chainIds;
	}

	@Override
	public void run() {
		try {
			name = Thread.currentThread().getName();
			process();
			log("vip.processNum " + name);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logError(e.getMessage());
			StackTraceElement[] elseElements =  e.getStackTrace();
			for (StackTraceElement ele: elseElements){
				logError(ele.toString());
			}
		} finally {
			close();
		}
		
	}
	
	private void process() throws SQLException {
		setLogFile(name);
		String chainIdString = "-1";
		for (int chainId : chainIds){
			chainIdString += ","+ chainId;
		}
		
		log("开始处理连锁店 : " + chainIdString);
		

		String sumVIP = "SELECT c.issue_chain_store_id,s.vip_card_id,c.vip_card_no, sum(coupon) from chain_vip_score s join chain_vip_cards c on s.vip_card_id = c.id where s.chain_id in ("+chainIdString+") and  date < '"+date1+"'  group by s.vip_card_id";
		String cleanVIPScore = "INSERT INTO chain_vip_score (chain_id, vip_card_id, vip_card_no, type, order_id, coupon, date, sales_value, comment) values (?,?,?,?,?,?,?,?,?)";
		String sumVIPMarch = "SELECT c.issue_chain_store_id,s.vip_card_id,c.vip_card_no, sum(coupon) from chain_vip_score s join chain_vip_cards c on s.vip_card_id = c.id where s.type='s' and s.chain_id in ("+chainIdString+") and date >'"+date1+"' and  date < '"+date2+"' and coupon < 0  group by s.vip_card_id";
		
		
    	PreparedStatement selectStatement = conn.prepareStatement(sumVIP);
    	PreparedStatement insertStatement = conn.prepareStatement(cleanVIPScore);
    	PreparedStatement selectMarchStatement = conn.prepareStatement(sumVIPMarch);

    	ResultSet result1 = null;
    	ResultSet result2 = null;
    	
    	result1 = selectStatement.executeQuery();
    	while (result1.next()){
			int chainId = result1.getInt(1);
			int vipCardId = result1.getInt(2);
			String cardNo = result1.getString(3);
			double sum = result1.getDouble(4);
			
			if (sum == 0)
				continue;
			
			insertStatement.setInt(1, chainId);
			insertStatement.setInt(2, vipCardId);
			insertStatement.setString(3, cardNo);
			insertStatement.setString(4, "m");
			insertStatement.setInt(5, 0);
			insertStatement.setDouble(6, sum * -1);
			
			java.sql.Date today = new java.sql.Date(2020-1900, 2, 31);
			insertStatement.setDate(7, today);
			insertStatement.setDouble(8, 0);
			insertStatement.setString(9, "清除3月前积分");
			
			log(chainId + "," + vipCardId + "," + cardNo);
			
			insertStatement.execute();
    	}
    	
    	result2 = selectMarchStatement.executeQuery();
    	while (result2.next()){
			int chainId = result2.getInt(1);
			int vipCardId = result2.getInt(2);
			String cardNo = result2.getString(3);
			double sum = result2.getDouble(4);
			
			if (sum == 0)
				continue;
			
			insertStatement.setInt(1, chainId);
			insertStatement.setInt(2, vipCardId);
			insertStatement.setString(3, cardNo);
			insertStatement.setString(4, "m");
			insertStatement.setInt(5, 0);
			insertStatement.setDouble(6, sum * -1);
			
			java.sql.Date today = new java.sql.Date(2020-1900, 2, 31);
			insertStatement.setDate(7, today);
			insertStatement.setDouble(8, 0);
			insertStatement.setString(9, "清除3月消费积分");
			
			log(chainId + "," + vipCardId + "," + cardNo);
			
			insertStatement.execute();
    	}
    	
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
