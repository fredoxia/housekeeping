package service;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import entity.ChainStore;

public class CleanVIPScoreImpl extends AbstractArchiving{

	public CleanVIPScoreImpl(String confFile) {
		super(confFile);
		setLogFile("vip积分清理");
	}
	
	public void process() throws SQLException{
		List<ChainStore> chainStores = getChainStores();
		Set<Integer> chainStoreIds = new HashSet<Integer>();
		
		String updateInitialValue = "UPDATE chain_vip_cards SET initial_value = 0";
		PreparedStatement updateInitialStatement = conn.prepareStatement(updateInitialValue);
    	int rows = updateInitialStatement.executeUpdate();
    	log("更新initial score : " + rows);
    	
		for (int i= 0; i< chainStores.size(); i++){
			if (i % 10 == 0){
				CleanVIPScore process = new CleanVIPScore(confFile);
		    	process.setChainIds(chainStoreIds);
				
		    	String threadName = "";
		    	Thread thread = new Thread(process);
		    	thread.setName("清理" + i );
		    	thread.start();
		    	threadName = thread.getName();
		    	
		    	log("正在处理    : " + threadName);
				
				
				chainStoreIds = new HashSet<Integer>();
			}
			
			chainStoreIds.add(chainStores.get(i).getChainId());
			
			
		}
		

	}

	public static void main(String[] args) throws SQLException {
		CleanVIPScoreImpl rArchiving = new CleanVIPScoreImpl("qxbabyConf");
		rArchiving.process();
	}

}
