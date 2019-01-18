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
 * 201805 锟斤拷锟斤拷 2011-2016
 * @author xiaf
 *
 */
public class SumZeroProcess extends AbstractArchiving implements Runnable{
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

	public SumZeroProcess(String confFile) {  
		   super(confFile);
	}  
	
	@Override
	public void run() {
		try {
			
			name = Thread.currentThread().getName();
			process();
			SumZeroArchivingImpl.addProcessNum(-1);
			log("SumZeroArchivingImpl.processNum " + SumZeroArchivingImpl.getProcessNum());
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

    public void process() throws SQLException{
    	setLogFile(name);
    	
    	logError(name + " " +chainStore.getChainName());
    	log(name +"---------------SumZeroArchiving " + houseKeepingConstant.getNewOrderId());

    	//3. 锟斤拷锟斤拷锟斤拷锟斤拷锟斤拷木锟斤拷锟较�
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
    	
//    	int index = 0;
//    	for (ChainStore chainStore :  chainStores){
//    		log("处理  : " + chainStore.getChainName() + " " + chainStore.getClientId()  + " "+ index++ + "/"+ chainSize);
    		int clientId = chainStore.getClientId();
    		
//    		Set<String> barcodes = chainStoreNotRequired.get(chainStore.getClientId());
    		int pbIdOriginal = -1;
    		
    		if (barcodes ==  null || barcodes.size() == 0){
    			log("barcode.siz = 0  Skip");
    		} else {
    			log(barcodes.toString());
	    		log("   barcodes : " + barcodes.size());
	    		
	    		int j = 0;
	    		for (String barcode : barcodes){
	    			j++;
	    			if (j % 50 == 0)
	    				log(name + " 条码 " + j +"/" + barcodes.size());
	    			
	    			pbIdOriginal = -1;
	    			selectStatement.setInt(1, clientId);
	    			selectStatement.setString(2, barcode);
	  
	    			result1 = selectStatement.executeQuery();
	    			java.sql.Date now = new java.sql.Date(new Date().getTime());
	    			while (result1.next()){
	    				//锟斤拷锟絘rchive锟斤拷锟角凤拷锟斤拷锟斤拷锟斤拷
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
	    	            try {
	    	               archiveStatement.execute();
	    	            } catch (Exception e){
	    	            	System.out.println(clientIdDB + "," + barcodeDB + "," + orderId);
	    	            }
	    	        }
	
	    			deleteStatement.setInt(1, clientId);
	    			deleteStatement.setString(2, barcode);
	    			
	    	        int record2 = deleteStatement.executeUpdate();
	    	        //log("删除 " + record2 + " 条码 " + barcode);
	    	        
	 	            dummyStatement.setInt(1, clientId);
		            dummyStatement.setString(2, barcode);
		            dummyStatement.setString(3, houseKeepingConstant.getNewOrderId());
		            dummyStatement.setInt(4, houseKeepingConstant.getType());
		            dummyStatement.setDate(5, now);
		            dummyStatement.setInt(6, pbIdOriginal);
		            dummyStatement.execute();
		            //log("插入 dummy record " + clientId + "," + barcode);
	    		}
	//    	}
    		}	    	
	    	
    		selectStatement.close();
        	deleteStatement.close();
        	dummyStatement.close();
        	archiveStatement.close();
        	archiveCountStatement.close();
        	
	    	houseKeepingConstant.updateNewOrderId();
	    	log(name + "---------------完成umZeroArchiving " + houseKeepingConstant.getNewOrderId());

    	
	}
		
	public static void main(String[] args) throws SQLException {
		SumZeroProcess rArchiving = new SumZeroProcess("qxbabyConf");
		
		rArchiving.process();

	}



}
