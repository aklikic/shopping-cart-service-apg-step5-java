package shopping.cart;

import akka.NotUsed;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.pubsub.Topic;
import akka.stream.javadsl.SourceQueue;
import shopping.cart.proto.*;

import java.util.List;
import java.util.stream.Collectors;

public class ShoppingCartConnectionActor extends AbstractBehavior<ShoppingCart.Event> {

    private final SourceQueue<CartEvent> downstream;
    public ShoppingCartConnectionActor(ActorContext<ShoppingCart.Event> context, ActorRef<Topic.Command<ShoppingCart.Event>> cartEventTopic,  SourceQueue<CartEvent> downstream) {
        super(context);
        this.downstream = downstream;
        getContext().getLog().info("Initialized!");
        cartEventTopic.tell(Topic.subscribe(context.getSelf()));

    }

    public static Behavior<ShoppingCart.Event> create(ActorRef<Topic.Command<ShoppingCart.Event>> cartEventTopic,SourceQueue<CartEvent> downstream) {
        return Behaviors.setup(ctx->new ShoppingCartConnectionActor(ctx,cartEventTopic,downstream));
    }

    @Override
    public Receive<ShoppingCart.Event> createReceive() {
        return newReceiveBuilder().onMessage(ShoppingCart.Event.class, this::onCartEvent).build();
    }

    private Behavior<ShoppingCart.Event> onCartEvent(ShoppingCart.Event event) {
        getContext().getLog().info("Sending event {}!", event);
        downstream.offer(toProtoCart(event));
        //TODO check if offer was successful and if not stop actor to kill connection
        return this;
    }

    private static CartEvent toProtoCart(ShoppingCart.Event event) {
        CartEvent.Builder cartEvent = CartEvent.newBuilder().setCartId(event.cartId);
        if(event instanceof ShoppingCart.ItemAdded){
            ShoppingCart.ItemAdded ev = (ShoppingCart.ItemAdded)event;
            cartEvent = cartEvent.setItemAdded(CartItemAdded.newBuilder().setItemId(ev.itemId).setQuantity(ev.quantity));
        }else if(event instanceof ShoppingCart.ItemRemoved){
            ShoppingCart.ItemRemoved ev = (ShoppingCart.ItemRemoved)event;
            cartEvent = cartEvent.setItemRemoved(CartItemRemoved.newBuilder().setItemId(ev.itemId).setOldQuantity(ev.oldQuantity));
        }else if(event instanceof ShoppingCart.ItemQuantityAdjusted){
            ShoppingCart.ItemQuantityAdjusted ev = (ShoppingCart.ItemQuantityAdjusted)event;
            cartEvent = cartEvent.setItemQuantityAdjusted(CartItemQuantityAdjusted.newBuilder().setNewQuantity(ev.newQuantity).setOldQuantity(ev.oldQuantity));
        }else if(event instanceof ShoppingCart.CheckedOut){
            ShoppingCart.CheckedOut ev = (ShoppingCart.CheckedOut)event;
            cartEvent = cartEvent.setCartCheckedOut(CartCheckedOut.newBuilder().setEventTime(ev.eventTime.toEpochMilli()));
        }

        return cartEvent.build();
    }

}
