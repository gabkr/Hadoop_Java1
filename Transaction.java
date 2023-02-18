public class Transaction {
    public int transID;
    public int custID;
    public float transTotal;
    public int transNumItems;
    public String transDesc;

    public Transaction (int transID, int custID, float transTotal, int transNumItems, String transDesc){
        this.transID = transID;
        this.custID = custID;
        this.transTotal = transTotal;
        this.transNumItems = transNumItems;
        this.transDesc = transDesc;
    }

    public int getTransID() {
        return transID;
    }

    public int getCustID() {
        return custID;
    }

    public float getTransTotal() {
        return transTotal;
    }

    public int getTransNumItems() {
        return transNumItems;
    }

    public String getTransDesc() {
        return transDesc;
    }

    public String toString(){
        return getTransID()+","+getCustID()+","+getTransTotal()+","+getTransNumItems()+","+getTransDesc()+"\n";
    }
}