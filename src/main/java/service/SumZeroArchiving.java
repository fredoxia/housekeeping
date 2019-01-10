package service;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import entity.ChainStore;

/**
 * 201805 处理 2011-2016
 * @author xiaf
 *
 */
public class SumZeroArchiving extends AbstractArchiving{

	public SumZeroArchiving(String confFile) {  
		   super(confFile);
	}  

    public void process() throws SQLException{
    	log("---------------开始SumZeroArchiving " + houseKeepingConstant.getNewOrderId());
    	
    	List<ChainStore> chainStores = new ArrayList<ChainStore>();
    	Map<Integer, Set<String>> chainStoreNotRequired = new HashMap<Integer, Set<String>>();

		//1. 获取chains
    	chainStores = getChainStores();
    	
    	//2. 获取每个连锁店的库存信息, sum=0的record
    	for (ChainStore chainStore :  chainStores){
    		Set<String> barcodes = new HashSet<String>();
    		
    		String getNotRequiredPB = "SELECT product_barcode FROM chain_in_out_stock WHERE client_id = ? GROUP BY product_barcode HAVING SUM(quantity)=0";
    		pStatement = conn.prepareStatement(getNotRequiredPB);
    		pStatement.setInt(1, chainStore.getClientId());
   
        	ResultSet chainStoreNotRequiredP = pStatement.executeQuery();
        	while (chainStoreNotRequiredP.next()){
        		String barcode = chainStoreNotRequiredP.getString("product_barcode");

        		barcodes.add(barcode);
        	}
        	
        	chainStoreNotRequired.put(chainStore.getClientId(), barcodes);
    	}
    	
    	//3. 处理连锁店的旧信息
    	String selectRow ="SELECT client_id, product_barcode, order_id, type, quantity, cost, sale_price, cost_total, sale_price_total, chain_sale_price_total, date, productBarcodeId FROM chain_in_out_stock WHERE  client_id =? AND product_barcode=?";
    	String deleteRow = "DELETE FROM chain_in_out_stock WHERE client_id =? AND product_barcode=?";
    	String dummyRecord = "INSERT INTO chain_in_out_stock (client_id, product_barcode, order_id, type, quantity, cost, sale_price, cost_total, sale_price_total, chain_sale_price_total, date, productBarcodeId) VALUES (?,?,?,?,0,0,0,0,0,0,?,?)";
    	String insertArchiveRecord = "INSERT INTO chain_in_out_stock_archive (client_id, product_barcode, order_id, type, quantity, cost, sale_price, cost_total, sale_price_total, chain_sale_price_total, date, productBarcodeId, archive_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";
    	String countArchiveRow = "SELECT COUNT(client_id) FROM chain_in_out_stock_archive WHERE client_id =? AND product_barcode=?";
    	
    	PreparedStatement selectStatement = conn.prepareStatement(selectRow);
    	PreparedStatement deleteStatement = conn.prepareStatement(deleteRow);
    	PreparedStatement dummyStatement = conn.prepareStatement(dummyRecord);
    	PreparedStatement archiveStatement = conn.prepareStatement(insertArchiveRecord);
    	PreparedStatement archiveCountStatement = conn.prepareStatement(countArchiveRow);
    	ResultSet result1 = null;
    	ResultSet result4 = null;
    	
    	for (ChainStore chainStore :  chainStores){
    		log("处理  : " + chainStore.getChainName() + " " + chainStore.getClientId());
    		int clientId = chainStore.getClientId();
    		
    		Set<String> barcodes = chainStoreNotRequired.get(chainStore.getClientId());
    		int pbIdOriginal = -1;
    		
    		if (barcodes.size() == 0){
    			log("barcode.siz = 0， Skip");
    			continue;
    		} else 
    			log(barcodes.toString());
    		
    		for (String barcode : barcodes){
    			pbIdOriginal = -1;
    			selectStatement.setInt(1, clientId);
    			selectStatement.setString(2, barcode);
  
    			result1 = selectStatement.executeQuery();
    			java.sql.Date now = new java.sql.Date(new Date().getTime());
    			while (result1.next()){
    				//检查archive表是否有数据
					archiveCountStatement.setInt(1, clientId);
					archiveCountStatement.setString(2, barcode);
	    			result4 = archiveCountStatement.executeQuery();
	    			int countArchive = 0;
	    			if (result4.next()){				
	    				countArchive = result4.getInt(1);
	    			}
	    			
    				int clientIdDB = result1.getInt(1);
    				String barcodeDB = result1.getString(2);
    				String orderId = result1.getString(3);
    				int type = result1.getInt(4);
    				
    	            if ((type == houseKeepingConstant.getType() || orderId.startsWith("ABA")) && countArchive>0 )
    	            	continue;
    	            
    	            int quantity = result1.getInt(5);
    	            double cost = result1.getDouble(6);
    	            double salePrice = result1.getDouble(7);
    	            double costTotal = result1.getDouble(8);
    	            double salePriceTotal = result1.getDouble(9);
    	            double chainSalePriceTotal = result1.getDouble(10);
    	            java.sql.Date date = result1.getDate(11);
    	            int pbId = result1.getInt(12);
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

    			deleteStatement.setInt(1, clientId);
    			deleteStatement.setString(2, barcode);
    			
    	        int record2 = deleteStatement.executeUpdate();
    	        log("删除 " + record2 + " 条数据 " + barcode);
    	        
 	            dummyStatement.setInt(1, clientId);
	            dummyStatement.setString(2, barcode);
	            dummyStatement.setString(3, houseKeepingConstant.getNewOrderId());
	            dummyStatement.setInt(4, houseKeepingConstant.getType());
	            dummyStatement.setDate(5, now);
	            dummyStatement.setInt(6, pbIdOriginal);
	            dummyStatement.executeUpdate();
	            log("补充 dummy record " + clientId + "," + barcode);
    		}
    	}
    	
    	close();
    	houseKeepingConstant.updateNewOrderId();
    	log("---------------完成开始SumZeroArchiving " + houseKeepingConstant.getNewOrderId());
    	
	}
		
	public static void main(String[] args) throws SQLException {
		SumZeroArchiving rArchiving = new SumZeroArchiving("qxbabyConf");
		
		rArchiving.process();

	}

}
