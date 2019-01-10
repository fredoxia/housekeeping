package service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class VerifyArchive {

	public static final String url = "jdbc:mysql://127.0.0.1/qxbabydb";  
	public static final String name = "com.mysql.jdbc.Driver";  
	public static final String user = "root";  
	public static final String password = "vj7683c6";  
	public static final String ORDER_ID = "ABA201805";
	public static final int TYPE = -9;
	
	
	public Connection conn = null;  
	
	public VerifyArchive() {  
	        try {  
	            Class.forName(name); 
	            conn = DriverManager.getConnection(url, user, password);

	        } catch (Exception e) {  
	            e.printStackTrace();  
	        }  
	    }  
	
	public void close() {  
	        try {  
	            this.conn.close();  
	        } catch (SQLException e) {  
	            e.printStackTrace();  
	        }  
	}  
	
	
	public void process() throws SQLException{
		Set<String> verified = new HashSet<String>();
	   	
    	ResultSet result1 = null;
    	ResultSet result2 = null;
    	ResultSet result3 = null;
		String processedRecords = "SELECT * FROM chain_in_out_stock";
		String verifyRecords = "SELECT SUM(quantity), SUM(cost_total), SUM(sale_price_total), SUM(chain_sale_price_total) FROM chain_in_out_stock_copy WHERE client_id =? AND product_barcode=?";
		
		PreparedStatement pStatement1 = conn.prepareStatement(processedRecords);
    	PreparedStatement pStatement2 = conn.prepareStatement(verifyRecords);
    	
    	result1 = pStatement1.executeQuery();
    	int i = 0;
    	while (result1.next()){
    		int quantity = result1.getInt("quantity");
    		double sumeCost = result1.getInt("cost_total");
    		double sumeSalePrice = result1.getInt("sale_price_total");
    		double sumeChainSales = result1.getInt("chain_sale_price_total");
    		int clientId = result1.getInt("client_id");
    		String barcode = result1.getString("product_barcode");
    		String orderId = result1.getString("order_id");
    		
    		if (verified.contains(clientId + "#" + barcode) || !orderId.startsWith("ABA"))
    			continue;
    		
    		pStatement2.setInt(1, clientId);
    		pStatement2.setString(2, barcode);
    		result2 = pStatement2.executeQuery();
    		
    		i++;
    		if (i % 5000 == 0)
    			System.out.println(i);
    		
    		if (result2.next()){
    			
        		int quantity2 = result2.getInt(1);
        		double sumeCost2 = result2.getInt(2);
        		double sumeSalePrice2 = result2.getInt(3);
        		double sumeChainSales2 = result2.getInt(4);
        		
        		if (quantity != quantity2 || Math.abs(sumeCost-sumeCost2)> 1 || Math.abs(sumeSalePrice-sumeSalePrice2)> 1 || Math.abs(sumeChainSales-sumeChainSales2)> 1){
        			System.out.println("Error : " + clientId + "," + barcode + "," + quantity + "," + quantity2 + "," + sumeCost + "," + sumeCost2 + "," + sumeSalePrice + "," +sumeSalePrice2);
        		} else {
        			
        			verified.add(clientId + "#" + barcode);
        			//System.out.println("Verified : " + clientId + "," + barcode + "," + quantity + "," + quantity2 + "," + sumeCost + "," + sumeCost2 + "," + sumeSalePrice + "," +sumeSalePrice2);
        		}
    		} else {
    			System.out.println("Error : " + clientId + "," + barcode);
    		}
    		
    	}

		
	}
		
	public static void main(String[] args) throws SQLException {
		VerifyArchive rArchiving = new VerifyArchive();
		
		rArchiving.process();
	
	}

}
