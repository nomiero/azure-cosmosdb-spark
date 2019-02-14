/**
  * The MIT License (MIT)
  * Copyright (c) 2016 Microsoft Corporation
  *
  * Permission is hereby granted, free of charge, to any person obtaining a copy
  * of this software and associated documentation files (the "Software"), to deal
  * in the Software without restriction, including without limitation the rights
  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  * copies of the Software, and to permit persons to whom the Software is
  * furnished to do so, subject to the following conditions:
  *
  * The above copyright notice and this permission notice shall be included in all
  * copies or substantial portions of the Software.
  *
  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  * SOFTWARE.
  */
package com.microsoft.azure.cosmosdb.spark.streaming

import com.microsoft.azure.cosmosdb.spark.LoggingTrait
import org.apache.spark.sql.cosmosdb.util.StreamingWriteTask
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.{DataFrame, SQLContext}
import com.microsoft.azure.cosmosdb.spark.config.Config

private[spark] class CosmosDBSink(sqlContext: SQLContext,
                                    configMap: Map[String, String])
  extends Sink with LoggingTrait with Serializable {

  private var lastBatchId: Long = -1L
  
  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= lastBatchId) {
      logDebug(s"Rerun batchId $batchId")
    } else {
      lastBatchId = batchId
    }

    val queryExecution: QueryExecution = data.queryExecution
    val schemaOutput = queryExecution.analyzed.output
    queryExecution.toRdd.foreachPartition( iter => {
      val writeTask = new StreamingWriteTask()
      writeTask.importStreamingData(iter, schemaOutput, Config(configMap))
    })
  }
}