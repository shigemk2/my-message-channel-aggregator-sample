package com.example

import akka.actor._
import com.example._

case class RequestForQuotation(rfqId: String, retailItems: Seq[RetailItem]) {
  val totalRetailPrice: Double = retailItems.map(retailItem => retailItem.retailPrice).sum
}

case class RetailItem(itemId: String, retailPrice: Double)

case class PriceQuoteInterest(quoteId: String, quoteProcessor: ActorRef, lowTotalRetail: Double, highTotalRetail: Double)

case class RequestPriceQuote(rfqId: String, itemId: String, retailPrice: Double, orderTotalRetailPrice: Double)

case class PriceQuote(quoterId: String, rfqId: String, itemId: String, retailPrice: Double, discountPrice: Double)

case class PriceQuoteFulfilled(priceQuote: PriceQuote)

case class RequiredPriceQuotesForFulfillment(rfqId: String, quotesRequested: Int)

case class QuotationFulfillment(rfqId: String, quotesRequested: Int, priceQuotes: Seq[PriceQuote], requester: ActorRef)

object AggregatorDriver {
  def main(args: Array[String]): Unit = {
  }
}
