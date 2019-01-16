package service;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import entity.ChainStore;
import entity.HouseKeepingConstant;

public abstract class AbstractArchiving {
	protected String url;  
	protected String name;
	protected String user;
	protected String password;
	protected String ORDER_ID;
	protected int TYPE;
	protected HouseKeepingConstant houseKeepingConstant;

	protected Connection conn = null;  
	protected PreparedStatement pStatement = null;
	private Logger log ; 
	private static Logger errorLogger;
	
	public Logger getErrorLogger(){
		if (errorLogger == null){
			errorLogger = Logger.getLogger("Error");
	        FileHandler fileHandler = null;
			try {
				fileHandler = new FileHandler("C:\\NotBackedUp\\AA-Error.log", true);
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
	        fileHandler.setLevel(Level.INFO); 
	        fileHandler.setFormatter(new ArchvingLogHander());
	        errorLogger.addHandler(fileHandler); 
		}
		
		return errorLogger;
	}
	
	
	public AbstractArchiving(String confFile) {  

		houseKeepingConstant = new HouseKeepingConstant(confFile);
		this.url = houseKeepingConstant.getUrl();
		this.name = houseKeepingConstant.getName();
		this.user = houseKeepingConstant.getUser();
		this.password = houseKeepingConstant.getPassword();
		this.ORDER_ID = houseKeepingConstant.getNewOrderId();
		this.TYPE = houseKeepingConstant.getType();
		
        try {  
            Class.forName(name); 
            conn = DriverManager.getConnection(url, user, password);

        } catch (Exception e) {  
            e.printStackTrace();  
        }  

	}  
	
	public void setLogFile(String fileName){
	        log = Logger.getLogger("AbstractArchiving" +fileName);
	        
//	        ConsoleHandler consoleHandler =new ConsoleHandler(); 
//	        consoleHandler.setLevel(Level.ALL); 
//	        log.addHandler(consoleHandler); 
//	        
	        FileHandler fileHandler = null;
			try {
				fileHandler = new FileHandler("C:\\NotBackedUp\\Archiving" + fileName + ".log", true);
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
	        fileHandler.setLevel(Level.INFO); 
	        fileHandler.setFormatter(new ArchvingLogHander());
	        log.addHandler(fileHandler); 
	}
	
	protected List<ChainStore> getChainStores() throws SQLException{
		List<ChainStore> chainStores = new ArrayList<ChainStore>();
    	
		
    	String getChains = "SELECT * FROM chain_store";
    	pStatement = conn.prepareStatement(getChains);
    	    	
    	ResultSet chainResult = pStatement.executeQuery();
    	while (chainResult.next()){
    		int chainId = chainResult.getInt("chain_id");
    		String chainName = chainResult.getString("chain_name");
    		int status = chainResult.getInt("status");
    		int clientId = chainResult.getInt("client_id");
    		
    		ChainStore chainStore = new ChainStore();
    		chainStore.setChainId(chainId);
    		chainStore.setChainName(chainName);
    		chainStore.setStatus(status);
    		chainStore.setClientId(clientId);
    		
    		chainStores.add(chainStore);
    	}
    	chainResult.close();

    	
    	//log(chainStores.size() + " " + chainStores.toString());
    	
    	return chainStores;
	}
	
	protected List<Integer> getQuarters() throws SQLException {
		List<Integer> quarters = new ArrayList<Integer>();
    	
		
    	String getQuarters = "SELECT quarter_ID FROM quarter";
    	pStatement = conn.prepareStatement(getQuarters);
    	    	
    	ResultSet quarterResult = pStatement.executeQuery();
    	while (quarterResult.next()){
    		int quarterId = quarterResult.getInt("quarter_ID");
    		
    		quarters.add(quarterId);
    	}
    	quarterResult.close();

    	//log(chainStores.size() + " " + chainStores.toString());
    	
    	return quarters;
	}
	
	public void log(String logInfo){
		log.info(logInfo);
	}
	
	public void logError(String erroLog){
		getErrorLogger().info(erroLog);
	}
	
	public void close() {  
        try {  
        	if ( this.pStatement != null)
        	    this.pStatement.close();
            this.conn.close();  
        } catch (SQLException e) {  
            e.printStackTrace();  
        }  
    }  
	
	class ArchvingLogHander extends Formatter { 
		private SimpleDateFormat dateFormat =  new SimpleDateFormat("yyyyMMdd hh:mm:ss");
        @Override 
        public String format(LogRecord record) { 
                return dateFormat.format(new Date()) + " - " + record.getMessage()+"\n"; 
        } 
}
}
