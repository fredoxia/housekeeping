package service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import entity.ChainStore;

public class RecordsArchivingImpl extends AbstractArchiving {
    private String confFile = "";
    private static int processNum = 0;
    
    public synchronized static void addProcessNum(int i){
    	processNum += i;
    }
    
    public synchronized static int getProcessNum(){
    	return processNum;
    }
    
	public RecordsArchivingImpl(String confFile) {
		super(confFile);
        this.confFile = confFile;
        setLogFile("RecordsArchiving主线程");
	}
	
	


	public void process() throws Exception{
    	List<ChainStore> chainStores = new ArrayList<ChainStore>();
    	List<Integer> quarters = new ArrayList<Integer>();
    	
     	String yearIds = houseKeepingConstant.getYearIds();
    	if (StringUtils.isEmpty(yearIds)){
    		logError("year 未设置");
    		throw new Exception("year 未设置");
    	}
    	log("获取年份: " + yearIds);
    	
		//1. 获取chains
    	chainStores = getChainStores();
    	
    	//2. 获取月份
    	quarters = getQuarters();
    	log("获取季度: " + quarters);
    	
		System.out.println("输入你的选择, 1 > 只是获取数据统计; 2 > 处理数据 ：");
		String userInforString ;
		do {
			Scanner in=new Scanner(System.in);
			userInforString = in.next();
		} while (!userInforString.equals("1") && !userInforString.equals("2"));
		
		if (userInforString.equals("1")) {
			try {
		    	for (ChainStore chainStore :  chainStores){
		    		Set<String> barcodes = new HashSet<String>();

		    		String getNotRequiredPB = "SELECT product_barcode FROM chain_in_out_stock WHERE client_id = ? AND product_barcode IN (SELECT barcode  FROM product_barcode pb WHERE pb.product_id IN (SELECT product_ID FROM product WHERE year_ID IN " + yearIds + ")) GROUP BY product_barcode HAVING SUM(quantity)=0 AND COUNT(product_barcode)>1";
		    		pStatement = conn.prepareStatement(getNotRequiredPB);
					pStatement.setInt(1, chainStore.getClientId());
					
		        	ResultSet chainStoreNotRequiredP = pStatement.executeQuery();
		        	while (chainStoreNotRequiredP.next()){
		        		String barcode = chainStoreNotRequiredP.getString("product_barcode");
		
		        		barcodes.add(barcode);
		        	}

		        	log( chainStore.getChainName() + " , " + barcodes.size());
		    		}
			} catch (Exception e ){
				e.printStackTrace();
				log(e.getMessage());
				StackTraceElement[] elseElements =  e.getStackTrace();
				for (StackTraceElement ele: elseElements){
					log(ele.toString());
				}
			} finally {
				close();
			}
		} else if (userInforString.equals("2")) {
			try {
				
			  	Map<Integer, Set<String>> chainStoreNotRequired = new HashMap<Integer, Set<String>>();
		    	Map<String, Integer> barcodeIdMap = new HashMap<String, Integer>();


		    	//2. productbarcode ids
		    	String getAllProductBarcode = "SELECT barcode, id  FROM product_barcode";
		    	pStatement = conn.prepareStatement(getAllProductBarcode);
		    	ResultSet pbRequired = pStatement.executeQuery();
		    	while (pbRequired.next()){
		    		String barcode = pbRequired.getString("barcode");
		    		int id = pbRequired.getInt("id");

		    		barcodeIdMap.put(barcode, id);
		    	}
		    	pbRequired.close();
		    	pStatement.close();

		    	log("year在数据库的值: " + yearIds);
		    	String getPBIdsRequired = "SELECT barcode  FROM product_barcode pb WHERE pb.product_id IN (SELECT product_ID FROM product WHERE year_ID IN " + yearIds + ")";
		    	
		    	for (ChainStore chainStore :  chainStores){
		    		Set<String> barcodes = new HashSet<String>();
		    		
		    		String getNotRequiredPB = "SELECT DISTINCT product_barcode FROM chain_in_out_stock WHERE client_id = ? AND product_barcode IN ( " + getPBIdsRequired + ")";
		    		pStatement = conn.prepareStatement(getNotRequiredPB);
		    		pStatement.setInt(1, chainStore.getClientId());
		   
		        	ResultSet chainStoreNotRequiredP = pStatement.executeQuery();
		        	while (chainStoreNotRequiredP.next()){
		        		String barcode = chainStoreNotRequiredP.getString("product_barcode");

		        		barcodes.add(barcode);
		        	}
		        	
		        	chainStoreNotRequired.put(chainStore.getClientId(), barcodes);
		    	}
		    	
		    	//2. , 处理数据
		    	for (ChainStore chainStore :  chainStores){
			    		while (getProcessNum() >= 50){
			    			log(">>>>>>>>>" + getProcessNum() + " , " +chainStore );
			    			Thread.sleep(300000);
			    		}
			    		
			    		Set<String> barcodes = chainStoreNotRequired.get(chainStore.getClientId());
			        	
			        	RecordsArchivingProcess process = new RecordsArchivingProcess(confFile);
			        	process.setBarcodes(barcodes);
			        	process.setChainStore(chainStore);
			        	
			        	String threadName = "";
			        	RecordsArchivingImpl.addProcessNum(1);
			        	Thread thread = new Thread(process);
			        	thread.setName("线程" + chainStore.getChainName());
			        	thread.start();
			        	threadName = thread.getName();
			        	
			        	log("启动线程    : " + threadName + " , " + getProcessNum() + " , " + barcodes.size());
		    		}
			} catch (Exception e ){
				e.printStackTrace();
				log(e.getMessage());
				StackTraceElement[] elseElements =  e.getStackTrace();
				for (StackTraceElement ele: elseElements){
					log(ele.toString());
				}
			} finally {
				close();
			}
		}
	}
	


	public static void main(String[] args) throws Exception {
		RecordsArchivingImpl rArchiving = new RecordsArchivingImpl("qxbabyConf");
		rArchiving.process();

	}

}
