package com.microsoft.azure.cosmosdb.spark.query

import java.util.Observable

import com.microsoft.azure.cosmosdb.{Document, FeedResponse}
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient

class IteratorWrapper[T](queryObservables: List[Observable[T]] ) extends Iterator[T]{
  override def hasNext: Boolean = {
    // Check if any observable has more results
  }

  override def next(): T = {
    // Get the next document in the list of iterators
  }
}
