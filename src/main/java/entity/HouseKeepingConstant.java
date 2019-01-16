package entity;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;


public class HouseKeepingConstant {
	private SimpleDateFormat dateFormat =  new SimpleDateFormat("yyyyMMddhh");
	private Properties properties = new Properties();
	private String newOrderId;
	
	public HouseKeepingConstant(String fileName){
		
    	try {
    		InputStream propertyFile = new FileInputStream("C:\\NotBackedUp\\" + fileName + ".properties");
    		
			properties.load(propertyFile);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Date now = new Date();
		newOrderId = "ABA"+ dateFormat.format(now);
	}
	
	public String getUrl(){
		return properties.getProperty("url");
	}
	
	public String getName(){
		return properties.getProperty("name");
	}
	public String getUser(){
		return properties.getProperty("user");
	}
	public String getPassword(){
		return properties.getProperty("password");
	}
	
	public String getOrderId(){
		String orderId = properties.getProperty("ORDER_ID");
		return orderId;
	}
	
	public String getYearIds(){
		return properties.getProperty("YearIds");
	}
	
	public String getNewOrderId(){

		return newOrderId;
	}
	
	public int getType(){
		return Integer.parseInt(properties.getProperty("TYPE"));
	}
	
	public void updateNewOrderId(){
		properties.setProperty("ORDER_ID", newOrderId);
	}
	
	public static void main(String[] args) throws SQLException {
		HouseKeepingConstant houseKeepingConstant = new HouseKeepingConstant("qxbabyConf");
		System.out.println(houseKeepingConstant.getNewOrderId());
		
//		houseKeepingConstant.updateNewOrderId();
//		
//		System.out.println(houseKeepingConstant.getOrderId());

	}
}
