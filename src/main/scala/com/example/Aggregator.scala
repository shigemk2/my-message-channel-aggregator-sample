package com.example

import akka.actor._
import com.example._

case class RequestForQuotation(rfqId: String, retailItems: Seq[RetailItem]) {
  val totalRetailPrice: Double = retailItems.map(retailItem => retailItem.retailPrice).sum
}

case class RetailItem(itemId: String, retailPrice: Double)

case class PriceQuoteInterest(quoterId: String, quoteProcessor: ActorRef, lowTotalRetail: Double, highTotalRetail: Double)

case class RequestPriceQuote(rfqId: String, itemId: String, retailPrice: Double, orderTotalRetailPrice: Double)

case class PriceQuote(quoterId: String, rfqId: String, itemId: String, retailPrice: Double, discountPrice: Double)

case class PriceQuoteFulfilled(priceQuote: PriceQuote)

case class RequiredPriceQuotesForFulfillment(rfqId: String, quotesRequested: Int)

case class QuotationFulfillment(rfqId: String, quotesRequested: Int, priceQuotes: Seq[PriceQuote], requester: ActorRef)

object AggregatorDriver extends CompletableApp(5) {
  val priceQuoteAggregator = system.actorOf(Props[PriceQuoteAggregator], "priceQuoteAggregator")
  val orderProcessor = system.actorOf(Props(classOf[MountaineeringSuppliesOrderProcessor], priceQuoteAggregator), "orderProcessor")

}

class MountaineeringSuppliesOrderProcessor(priceQuoteAggregator: ActorRef) extends Actor {
  val interestRegistry = scala.collection.mutable.Map[String, PriceQuoteInterest]()

  def calculateRecipientList(rfq: RequestForQuotation): Iterable[ActorRef] = {
    for {
      interest <- interestRegistry.values
      if (rfq.totalRetailPrice >= interest.lowTotalRetail)
      if (rfq.totalRetailPrice <= interest.highTotalRetail)
    } yield interest.quoteProcessor
  }

  def dispatchTo(rfq: RequestForQuotation, recipientList: Iterable[ActorRef]) = {
    var totalRequestedQuotes = 0
    recipientList.foreach { recipient =>
      rfq.retailItems.foreach { retailItem =>
        println("OrderProcessor: " + rfq.rfqId + " item: " + retailItem.itemId + " to: " + recipient.path.toString)
        recipient ! RequestPriceQuote(rfq.rfqId, retailItem.itemId, retailItem.retailPrice, rfq.totalRetailPrice)
      }
    }
  }

  def receive = {
    case interest: PriceQuoteInterest =>
      interestRegistry(interest.quoterId) = interest
    case priceQuote: PriceQuote =>
      priceQuoteAggregator ! PriceQuoteFulfilled(priceQuote)
      println(s"OrderProcessor: received: $priceQuote")
    case rfq: RequestForQuotation =>
      val recipientList = calculateRecipientList(rfq)
      priceQuoteAggregator ! RequiredPriceQuotesForFulfillment(rfq.rfqId, recipientList.size * rfq.retailItems.size)
      dispatchTo(rfq, recipientList)
    case fulfillment: QuotationFulfillment =>
      println(s"OrderProcessor: received: $fulfillment")
      AggregatorDriver.completedStep()
    case message: Any =>
      println(s"OrderProcessor: received unexpected message: $message")
  }
}

class PriceQuoteAggregator extends Actor {
  val fulfilledPriceQuotes = scala.collection.mutable.Map[String, QuotationFulfillment]()

  def receive = {
    case required: RequiredPriceQuotesForFulfillment =>
      fulfilledPriceQuotes(required.rfqId) = QuotationFulfillment(required.rfqId, required.quotesRequested, Vector(), sender())
    case priceQuoteFulFilled: PriceQuoteFulfilled =>
      val previousFulfillment = fulfilledPriceQuotes(priceQuoteFulFilled.priceQuote.rfqId)
      val currentPriceQuotes = previousFulfillment.priceQuotes :+ priceQuoteFulFilled.priceQuote
      val currentFulfillment =
        QuotationFulfillment(
          previousFulfillment.rfqId,
          previousFulfillment.quotesRequested,
          currentPriceQuotes,
          previousFulfillment.requester
        )

      if (currentPriceQuotes.size >= currentFulfillment.quotesRequested) {
        currentFulfillment.requester ! currentFulfillment
        fulfilledPriceQuotes.remove(priceQuoteFulFilled.priceQuote.rfqId)
      } else {
        fulfilledPriceQuotes(priceQuoteFulFilled.priceQuote.rfqId) = currentFulfillment
      }

      println(s"PriceQuoteAggregator: fulfilled price quote: $priceQuoteFulFilled")
    case message: Any =>
      println(s"PriceQuoteAggregator: received unexpected message: $message")
  }
}