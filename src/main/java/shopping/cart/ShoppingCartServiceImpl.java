package shopping.cart;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.pubsub.Topic;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.grpc.GrpcServiceException;
import akka.japi.JavaPartialFunction;
import akka.japi.pf.PFBuilder;
import akka.persistence.jdbc.query.javadsl.JdbcReadJournal;
import akka.persistence.query.EventEnvelope;
import akka.persistence.typed.PersistenceId;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.Merge;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.SourceQueue;
import akka.stream.javadsl.SourceQueueWithComplete;
import akka.stream.typed.javadsl.ActorSource;
import io.grpc.Status;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.PartialFunction;
import shopping.cart.proto.*;

public final class ShoppingCartServiceImpl implements ShoppingCartService {

  private static final Logger logger = LoggerFactory.getLogger(ShoppingCartServiceImpl.class);

  private final ActorRef<Topic.Command<ShoppingCart.Event>> cartEventTopic;
  private final Duration timeout;
  private final ClusterSharding sharding;
  private final ActorSystem<?> system;
  private final ActorContext<?> context;
  private final JdbcReadJournal readJournal;

  public ShoppingCartServiceImpl(ActorSystem<?> system, ActorContext<?> context, akka.actor.typed.ActorRef<Topic.Command<ShoppingCart.Event>> cartEventTopic,JdbcReadJournal readJournal) {
    this.cartEventTopic = cartEventTopic;
    timeout = system.settings().config().getDuration("shopping-cart-service.ask-timeout");
    sharding = ClusterSharding.get(system);
    this.system = system;
    this.context = context;
    this.readJournal = readJournal;
  }

  @Override
  public CompletionStage<Cart> addItem(AddItemRequest in) {
    logger.info("addItem {} to cart {}", in.getItemId(), in.getCartId());
    EntityRef<ShoppingCart.Command> entityRef =
        sharding.entityRefFor(ShoppingCart.ENTITY_KEY, in.getCartId());
    CompletionStage<ShoppingCart.Summary> reply =
        entityRef.askWithStatus(
            replyTo -> new ShoppingCart.AddItem(in.getItemId(), in.getQuantity(), replyTo),
            timeout);
    CompletionStage<Cart> cart = reply.thenApply(ShoppingCartServiceImpl::toProtoCart);
    return convertError(cart);
  }

  @Override
  public CompletionStage<Cart> updateItem(UpdateItemRequest in) {
    logger.info("updateItem {}", in.getCartId());
    EntityRef<ShoppingCart.Command> entityRef =
        sharding.entityRefFor(ShoppingCart.ENTITY_KEY, in.getCartId());
    final CompletionStage<ShoppingCart.Summary> reply;
    if (in.getQuantity() == 0) {
      reply =
          entityRef.askWithStatus(
              replyTo -> new ShoppingCart.RemoveItem(in.getItemId(), replyTo), timeout);
    } else {
      reply =
          entityRef.askWithStatus(
              replyTo ->
                  new ShoppingCart.AdjustItemQuantity(in.getItemId(), in.getQuantity(), replyTo),
              timeout);
    }
    CompletionStage<Cart> cart = reply.thenApply(ShoppingCartServiceImpl::toProtoCart);
    return convertError(cart);
  }

  
  @Override
  public CompletionStage<Cart> checkout(CheckoutRequest in) {
    logger.info("checkout {}", in.getCartId());
    EntityRef<ShoppingCart.Command> entityRef =
        sharding.entityRefFor(ShoppingCart.ENTITY_KEY, in.getCartId());
    CompletionStage<ShoppingCart.Summary> reply =
        entityRef.askWithStatus(replyTo -> new ShoppingCart.Checkout(replyTo), timeout);
    CompletionStage<Cart> cart = reply.thenApply(ShoppingCartServiceImpl::toProtoCart);
    return convertError(cart);
  }

  @Override
  public CompletionStage<Cart> getCart(GetCartRequest in) {
    logger.info("getCart {}", in.getCartId());
    EntityRef<ShoppingCart.Command> entityRef =
        sharding.entityRefFor(ShoppingCart.ENTITY_KEY, in.getCartId());
    CompletionStage<ShoppingCart.Summary> reply =
        entityRef.ask(replyTo -> new ShoppingCart.Get(replyTo), timeout);
    CompletionStage<Cart> protoCart =
        reply.thenApply(
            cart -> {
              if (cart.items.isEmpty())
                throw new GrpcServiceException(
                    Status.NOT_FOUND.withDescription("Cart " + in.getCartId() + " not found"));
              else return toProtoCart(cart);
            });
    return convertError(protoCart);
  }


  @Override
  public Source<CartEvent, NotUsed> itemStream(ItemStreamRequest in) {
    logger.info("itemStream Connection established.");
    //TODO stream complete and error handling
    final Source<ShoppingCart.Event, ActorRef<ShoppingCart.Event>> source =
            ActorSource.<ShoppingCart.Event>actorRef(
                    (m) -> false,
                    (m)-> Optional.empty(),
                    100,
                    OverflowStrategy.fail());

    return
    source.mapMaterializedValue(actor->{
      cartEventTopic.tell(Topic.subscribe(actor));
      return NotUsed.getInstance();
    }).map(ShoppingCartServiceImpl::toProtoCart);

  }

  @Override
  public Source<CartEvent, NotUsed> itemStreamJournal(ItemStreamJournalRequest in) {
    logger.info("itemStreamJournal Connection established: {}",in.toString());
    List<Source<EventEnvelope,?>> sources =
    in.getCartSeqNumList().stream()
            .map(cartSeqNum -> readJournal.eventsByPersistenceId(PersistenceId.of(ShoppingCart.ENTITY_KEY.name(), cartSeqNum.getCartId()).id(), cartSeqNum.getEventSeqNum(), Long.MAX_VALUE))
            .collect(Collectors.toList());

    Source<EventEnvelope,?> combined = Source.from(sources).flatMapMerge(sources.size(),source->source);

    return
    combined
    .mapMaterializedValue(m->NotUsed.getInstance())
    .map(ShoppingCartServiceImpl::toProtoCart);
  }

  private static CartEvent toProtoCart(EventEnvelope event) {
    return toProtoCart((ShoppingCart.Event) event.event(),event.sequenceNr());
  }
  private static CartEvent toProtoCart(ShoppingCart.Event event) {
    return toProtoCart(event,-1);
  }
  private static CartEvent toProtoCart(ShoppingCart.Event event,long seqNumber) {
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
    if(seqNumber>-1)
      cartEvent = cartEvent.setEventSeqNum(seqNumber);
    return cartEvent.build();
  }
  private static Cart toProtoCart(ShoppingCart.Summary cart) {
    List<Item> protoItems =
        cart.items.entrySet().stream()
            .map(
                entry ->
                    Item.newBuilder()
                        .setItemId(entry.getKey())
                        .setQuantity(entry.getValue())
                        .build())
            .collect(Collectors.toList());

    return Cart.newBuilder().setCheckedOut(cart.checkedOut).addAllItems(protoItems).build();
  }
  

  private static <T> CompletionStage<T> convertError(CompletionStage<T> response) {
    return response.exceptionally(
        ex -> {
          if (ex instanceof TimeoutException) {
            throw new GrpcServiceException(
                Status.UNAVAILABLE.withDescription("Operation timed out"));
          } else {
            throw new GrpcServiceException(
                Status.INVALID_ARGUMENT.withDescription(ex.getMessage()));
          }
        });
  }
}
