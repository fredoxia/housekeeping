package entity;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;



public class HouseKeepingConstant {
	private SimpleDateFormat dateFormat =  new SimpleDateFormat("yyyyMMdd");
	private PropertiesConfiguration properties = new PropertiesConfiguration();
	private String newOrderId;
	
	public HouseKeepingConstant(String fileName){
		
    	try {
			properties = new PropertiesConfiguration("C:\\NotBackedUp\\" + fileName + ".properties");
		} catch (ConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        properties.setAutoSave(true);
		Date now = new Date();
		newOrderId = "ABA"+ dateFormat.format(now);
	}
	
	public String getUrl(){
		return properties.getString("url");
	}
	
	public String getName(){
		return properties.getString("name");
	}
	public String getUser(){
		return properties.getString("user");
	}
	public String getPassword(){
		return properties.getString("password");
	}
	
	public String getOrderId(){
		String orderId = properties.getString("ORDER_ID");
		return orderId;
	}
	
	public String getYearIds(){
		return properties.getString("YearIds");
	}
	
	public String getNewOrderId(){

		return newOrderId;
	}
	
	public int getType(){
		return properties.getInt("TYPE");
	}
	
	public void updateNewOrderId(){
		properties.setProperty("ORDER_ID", newOrderId);
	}
	
	public static void main(String[] args) throws SQLException {
		HouseKeepingConstant houseKeepingConstant = new HouseKeepingConstant("qxbabyConf");
		System.out.println(houseKeepingConstant.getYearIds());
		
//		houseKeepingConstant.updateNewOrderId();
//		
//		System.out.println(houseKeepingConstant.getOrderId());

	}
}
