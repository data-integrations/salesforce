/*
 * Copyright © 2022 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.salesforce.source

import com.google.gson.Gson
import io.cdap.cdap.api.data.format.StructuredRecord
import io.cdap.cdap.etl.api.streaming.StreamingEventHandler
import io.cdap.plugin.salesforce.plugin.source.streaming.JSONRecord
import org.apache.commons.lang3.ArrayUtils
import org.apache.spark.api.java.function.{Function2, VoidFunction}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Duration, StreamingContext, Time}
import org.slf4j.LoggerFactory

/**
 * DStream that implements {@link StreamingEventHandler} .
 * This DStream will keep the Kafka offsets for each batch RDD before applying the _transformFunction.
 * On calling onBatchCompleted, the _stateConsumer will be provided with these offsets.
 *
 * @param _ssc           Spark streaming context
 * @param _salesforceDStream  InputDStream created through SalesforceReceiver
 * @param _transformFunction Transform function for converting JSONRecord to StructuredRecord                           *
 * @param _stateConsumer Consumer function for the state produced
 */
class SalesforceDStream(_ssc: StreamingContext,
                        _salesforceDStream: InputDStream[String],
                        _transformFunction: Function2[String, Time, StructuredRecord],
                        _stateConsumer: VoidFunction[Int]
                       )
  extends DStream[StructuredRecord](_ssc) with StreamingEventHandler {
  private val LOG = LoggerFactory.getLogger(classOf[SalesforceDStream])

  // For storing the replayId in each batch
  private var replayId: Int = -1

  // For keeping the records in each batch
  private var jsonRecordArray : Array[Object]  = Array[Object]()

  private var jsonRecord : JSONRecord = null

  override def slideDuration: Duration = _salesforceDStream.slideDuration

  override def dependencies: List[DStream[_]] = List(_salesforceDStream)

  override def compute(validTime: Time): Option[RDD[StructuredRecord]] = {
    LOG.info(">>>> In SalesforceDStream compute()")
    val rddOption = _salesforceDStream.compute(validTime)
    val transformFn = _transformFunction
    // If there is a RDD produced, cache the replayId for the batch and then transform to RDD[StructuredRecord]
    rddOption.map(rdd => {
      jsonRecordArray = rdd.collect().asInstanceOf[Array[Object]]
      if (ArrayUtils.isNotEmpty(jsonRecordArray)) {
        jsonRecord = new Gson().fromJson(jsonRecordArray.last.toString, classOf[JSONRecord])
        replayId = jsonRecord.getEvent.getReplayId
      } else {
        replayId = -1
      }
      rdd.map(record => transformFn.call(record, validTime))
    })

  }

  override def onBatchCompleted(context: io.cdap.cdap.etl.api.streaming.StreamingContext): Unit = {
    _stateConsumer.call(replayId)
  }

  /**
   * Convert this to a {@link JavaDStream}
   *
   * @return JavaDStream
   */
  def convertToJavaDStream(): JavaDStream[StructuredRecord] = {
    JavaDStream.fromDStream(this)
  }
}
