package houseKeeping.entity;

import java.util.Date;


public class ChainInOutStock {
	private String barcode;
    private int clientId;
    private String orderId;
    private int type;
    private double cost;
    private double costTotal;
    private double salePrice;
    private double salePriceTotal;
    private double chainSalePriceTotal;
    private int quantity;

    
	  public ChainInOutStock(){
	    	
	   }
	    
	    public ChainInOutStock(String barcode, int clientId, String orderId, int type, double cost, double costTotal, double salePrice, double salePriceTotal, double chainSalePriceTotal, int quantity){
	    	this.barcode = barcode;
	    	this.clientId = clientId;
	    	this.orderId = orderId;
	    	this.type = type;
	    	this.cost = cost;
	    	this.costTotal = costTotal;
	    	this.salePrice = salePrice;
	    	this.salePriceTotal = salePriceTotal;
	    	this.quantity = quantity;
	    	this.chainSalePriceTotal = chainSalePriceTotal;

	    }
	    
	    public String getKey(){
	    	return barcode +"#" + clientId;
	    }
	    
		public double getChainSalePriceTotal() {
			return chainSalePriceTotal;
		}

		public void setChainSalePriceTotal(double chainSalePriceTotal) {
			this.chainSalePriceTotal = chainSalePriceTotal;
		}

		public int getType() {
			return type;
		}

		public void setType(int type) {
			this.type = type;
		}

		public String getBarcode() {
			return barcode;
		}

		public void setBarcode(String barcode) {
			this.barcode = barcode;
		}

		public int getClientId() {
			return clientId;
		}
		public void setClientId(int clientId) {
			this.clientId = clientId;
		}
		public String getOrderId() {
			return orderId;
		}
		public void setOrderId(String orderId) {
			this.orderId = orderId;
		}
		public double getCost() {
			return cost;
		}
		public void setCost(double cost) {
			this.cost = cost;
		}
		public double getCostTotal() {
			return costTotal;
		}
		public void setCostTotal(double costTotal) {
			this.costTotal = costTotal;
		}
		public double getSalePrice() {
			return salePrice;
		}
		public void setSalePrice(double salePrice) {
			this.salePrice = salePrice;
		}
		public double getSalePriceTotal() {
			return salePriceTotal;
		}
		public void setSalePriceTotal(double salePriceTotal) {
			this.salePriceTotal = salePriceTotal;
		}
		public int getQuantity() {
			return quantity;
		}
		public void setQuantity(int quantity) {
			this.quantity = quantity;
		}
		
		public void add(ChainInOutStock stockInMap) {
			this.quantity += stockInMap.getQuantity();
			this.salePriceTotal += stockInMap.getSalePriceTotal();
			this.costTotal += stockInMap.getCostTotal();
			
		}

		

}
