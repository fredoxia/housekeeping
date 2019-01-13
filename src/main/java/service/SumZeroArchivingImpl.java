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

public class SumZeroArchivingImpl extends AbstractArchiving {
    private String confFile = "";
    private static int processNum = 0;
    
    public synchronized static void addProcessNum(int i){
    	processNum += i;
    }
    
    public synchronized static int getProcessNum(){
    	return processNum;
    }
    
	public SumZeroArchivingImpl(String confFile) {
		super(confFile);
        this.confFile = confFile;
        setLogFile("主线程");
	}
	
	public List<ChainStore> getChainStores() throws SQLException{
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
	
	private List<Integer> getQuarters() throws SQLException {
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
		    	//2. , sum=0锟斤拷record
		    	for (Integer quarterId : quarters){
		    		for (ChainStore chainStore :  chainStores){
			    		while (getProcessNum() >= 50){
			    			log(">>>>>>>>>" + getProcessNum() + " , " + quarterId + " , " +chainStore );
			    			Thread.sleep(300000);
			    		}
			    		
			    		Set<String> barcodes = new HashSet<String>();
	
			    		String getNotRequiredPB = "SELECT product_barcode FROM chain_in_out_stock WHERE client_id = ? AND product_barcode IN (SELECT barcode  FROM product_barcode pb WHERE pb.product_id IN (SELECT product_ID FROM product WHERE quarter_ID =? AND year_ID IN " + yearIds + ")) GROUP BY product_barcode HAVING SUM(quantity)=0 AND COUNT(product_barcode)>1";
			    		pStatement = conn.prepareStatement(getNotRequiredPB);
			
						pStatement.setInt(1, chainStore.getClientId());
						pStatement.setInt(2, quarterId);
						
			        	ResultSet chainStoreNotRequiredP = pStatement.executeQuery();
			        	while (chainStoreNotRequiredP.next()){
			        		String barcode = chainStoreNotRequiredP.getString("product_barcode");
			
			        		barcodes.add(barcode);
			        	}
			        	
			        	
			        	SumZeroProcess process = new SumZeroProcess(confFile);
			        	process.setBarcodes(barcodes);
			        	process.setChainStore(chainStore);
			        	
			        	String threadName = "";
			        	SumZeroArchivingImpl.addProcessNum(1);
			        	Thread thread = new Thread(process);
			        	thread.setName("线程" + chainStore.getChainName() + "-" + quarterId);
			        	thread.start();
			        	threadName = thread.getName();
			        	
			        	log("启动线程    : " + threadName + " , " + getProcessNum() + " , " + barcodes.size());
		    		}
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
		SumZeroArchivingImpl rArchiving = new SumZeroArchivingImpl("qxbabyConf");
		rArchiving.process();

	}

}
