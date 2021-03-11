package shopping.cart;

public class PubSubEvent implements CborSerializable{
    public ShoppingCart.Event event;
    public long eventSeqNum;
    public PubSubEvent(ShoppingCart.Event event,long eventSeqNum){
        this.event=event;
        this.eventSeqNum=eventSeqNum;
    }

}
