package service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import entity.ChainStore;
import entity.HouseKeepingConstant;

/**
 * 
 * @author xiaf
 *
 */
public class RecordsArchivingProcess extends AbstractArchiving  implements Runnable{
	   private ChainStore chainStore;
	    private Set<String> barcodes;
		private String name;
		
		public ChainStore getChainStore() {
			return chainStore;
		}

		public void setChainStore(ChainStore chainStore) {
			this.chainStore = chainStore;
		}

		public Set<String> getBarcodes() {
			return barcodes;
		}

		public void setBarcodes(Set<String> barcodes) {
			this.barcodes = barcodes;
		}
		
	public RecordsArchivingProcess(String confFile) {  
         super(confFile);
	} 
	
	@Override
	public void run() {
		try {
			
			name = Thread.currentThread().getName();
			process();
			RecordsArchivingImpl.addProcessNum(-1);
			log("RecordsArchivingImpl.processNum " + RecordsArchivingImpl.getProcessNum());
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
		
    public void process() throws Exception{
    	setLogFile(name);
    	
    	log("---------------recordsArchiving " + houseKeepingConstant.getNewOrderId());
    	int chainSize = 0;
  
    	
    	//4. 
    	String selectRow ="SELECT client_id, product_barcode, order_id, type, quantity, cost, sale_price, cost_total, sale_price_total, chain_sale_price_total, date, productBarcodeId FROM chain_in_out_stock WHERE  client_id =? AND product_barcode=?";
    	String deleteRow = "DELETE FROM chain_in_out_stock WHERE client_id =? AND product_barcode=?";
    	String countRow = "SELECT COUNT(client_id) FROM chain_in_out_stock WHERE client_id =? AND product_barcode=?";
    	String getQSum = "SELECT SUM(quantity) FROM chain_in_out_stock WHERE client_id =? AND product_barcode=?";
    	String getSum = "SELECT SUM(quantity), SUM(cost_total), SUM(sale_price_total), SUM(chain_sale_price_total) FROM chain_in_out_stock WHERE client_id =? AND product_barcode=?";
    	String newRecord = "INSERT INTO chain_in_out_stock (client_id, product_barcode, order_id, type, quantity, cost, sale_price, cost_total, sale_price_total, chain_sale_price_total, date, productBarcodeId) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
    	String insertArchiveRecord = "INSERT INTO chain_in_out_stock_archive (client_id, product_barcode, order_id, type, quantity, cost, sale_price, cost_total, sale_price_total, chain_sale_price_total, date, productBarcodeId, archive_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";
    	String countArchiveRow = "SELECT COUNT(client_id) FROM chain_in_out_stock_archive WHERE client_id =? AND product_barcode=?";
    	
    	PreparedStatement selectStatement = conn.prepareStatement(selectRow);
    	PreparedStatement countRowsStatement = conn.prepareStatement(countRow);
    	PreparedStatement sumAllStatement = conn.prepareStatement(getSum);
    	PreparedStatement sumQStatement = conn.prepareStatement(getQSum);
    	PreparedStatement deleteRowStatement = conn.prepareStatement(deleteRow);
    	PreparedStatement newStockRowStatement = conn.prepareStatement(newRecord);
    	PreparedStatement archiveStatement = conn.prepareStatement(insertArchiveRecord);
    	PreparedStatement archiveCountStatement = conn.prepareStatement(countArchiveRow);
    	
    	ResultSet result1 = null;
    	ResultSet result2 = null;
    	ResultSet result3 = null;
    	ResultSet result4 = null;
    	int index = 0;

    	int clientId = chainStore.getClientId();
    	
		log("  需要处理的条码 : " + barcodes.size());
		for (String barcode : barcodes){
			int pbIdOriginal = -1;
			countRowsStatement.setInt(1, clientId);
			countRowsStatement.setString(2, barcode);
			
			result1 = countRowsStatement.executeQuery();
			if (result1.next()){				
				int count = result1.getInt(1);
				
				//log("  ����  : " + barcode + "," + count);
				if (count > 1){
					archiveCountStatement.setInt(1, clientId);
					archiveCountStatement.setString(2, barcode);
	    			result4 = archiveCountStatement.executeQuery();
	    			int countArchive = 0;
	    			if (result4.next()){				
	    				countArchive = result4.getInt(1);
	    			}

	    			
	    			//获取每条数据，然后插入到archive表
					selectStatement.setInt(1, clientId);
        			selectStatement.setString(2, barcode);
      
        			result2 = selectStatement.executeQuery();
        			java.sql.Date now = new java.sql.Date(new Date().getTime());
        			while (result2.next()){
        				int clientIdDB = result2.getInt(1);
        				String barcodeDB = result2.getString(2);
        				String orderId = result2.getString(3);
        				int type = result2.getInt(4);
        				
        				if (type == houseKeepingConstant.getType() || orderId.startsWith("ABA") && countArchive>0 )
        					continue;

        	            int quantity = result2.getInt(5);
        	            double cost = result2.getDouble(6);
        	            double salePrice = result2.getDouble(7);
        	            double costTotal = result2.getDouble(8);
        	            double salePriceTotal = result2.getDouble(9);
        	            double chainSalePriceTotal = result2.getDouble(10);
        	            java.sql.Date date = result2.getDate(11);
        	            int pbId = result2.getInt(12);
        	            pbIdOriginal = pbId;

        	            archiveStatement.setInt(1, clientIdDB);
        	            archiveStatement.setString(2, barcodeDB);
        	            archiveStatement.setString(3, orderId);
        	            archiveStatement.setInt(4, type);
        	            archiveStatement.setInt(5, quantity);
        	            archiveStatement.setDouble(6, cost);
        	            archiveStatement.setDouble(7, salePrice);
        	            archiveStatement.setDouble(8, costTotal);
        	            archiveStatement.setDouble(9, salePriceTotal);
        	            archiveStatement.setDouble(10, chainSalePriceTotal);
        	            archiveStatement.setDate(11, date);
        	            archiveStatement.setInt(12, pbId);
        	            archiveStatement.setDate(13, now);
        	            archiveStatement.execute();
        	        }

        			
					//获取这几条数据的sum信息
	    			sumAllStatement.setInt(1, clientId);
	    			sumAllStatement.setString(2, barcode);
	    			result2 = sumAllStatement.executeQuery();
	    			int quantity = 0;
	    			double costTotal = 0;
	    			double salePriceTotal = 0;
	    			double chainSalePriceTotal = 0;
	    			
	            	if (result2.next()){
	            		quantity = result2.getInt(1);
	            		costTotal = result2.getDouble(2);
	            		salePriceTotal = result2.getDouble(3);
	            		chainSalePriceTotal = result2.getDouble(4);
	            	}
	            	
	            	//删除原数据
	            	deleteRowStatement.setInt(1, clientId);
    	    		deleteRowStatement.setString(2, barcode);
    				deleteRowStatement.executeUpdate();
    				
    				//如果quantity不为0，插入新的汇总数据
    				if (quantity != 0)	{
    					newStockRowStatement.setInt(1, chainStore.getClientId());
    					newStockRowStatement.setString(2, barcode);
    					newStockRowStatement.setString(3, ORDER_ID);
    					newStockRowStatement.setInt(4, TYPE);
    					newStockRowStatement.setInt(5, quantity);
    					newStockRowStatement.setDouble(6, costTotal/quantity);
    					newStockRowStatement.setDouble(7, salePriceTotal/quantity);
    					newStockRowStatement.setDouble(8, costTotal);
    					newStockRowStatement.setDouble(9, salePriceTotal);
    					newStockRowStatement.setDouble(10, chainSalePriceTotal);
    					newStockRowStatement.setTimestamp(11, new Timestamp(new java.util.Date().getTime()));
    					newStockRowStatement.setInt(12, pbIdOriginal);
					
    					newStockRowStatement.execute();
	            	}
					
				} else if (count == 1){
	    			sumQStatement.setInt(1, clientId);
	    			sumQStatement.setString(2, barcode);
	    			result3 = sumQStatement.executeQuery();
	    			if (result3.next()){
	    				int Qsum = result3.getInt(1);
	    				if (Qsum == 0){
	     	    			deleteRowStatement.setInt(1, clientId);
	    	    			deleteRowStatement.setString(2, barcode);
	    					deleteRowStatement.executeUpdate();
	    				}
	    			}
				}
			}
		
		}

    	
    	close();
    	houseKeepingConstant.updateNewOrderId();
    	log("---------------完成RecordsArchiving " + houseKeepingConstant.getNewOrderId());
    	
	}
		
	public static void main(String[] args) throws Exception {
		RecordsArchivingProcess rArchiving = new RecordsArchivingProcess("qxbabyConf");
		
		rArchiving.process();

	}



}
