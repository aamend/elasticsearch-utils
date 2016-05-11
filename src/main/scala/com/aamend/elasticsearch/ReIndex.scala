package com.aamend.elasticsearch

import java.net.InetAddress

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilders._
import ReIndex.SearchResponse

/**
  * Created by antoine on 10/05/2016.
  */
class ReIndex extends LazyLogging {

  val config = ConfigFactory.load()
  val hostname = config.getString("host")
  val port = config.getInt("port")
  val cluster = config.getString("cluster")
  val timeValue = config.getInt("timeValue")
  val batchSize = config.getInt("batchSize")
  val username = config.getString("username")
  val password = config.getString("password")

  def process(fromIndexName: String, fromIndexType: String, toIndexName: String, toIndexType: String) = {

    logger.info(s"Exporting data from $fromIndexName/$fromIndexType")
    logger.info(s"Importing data to $toIndexName/$toIndexType")
    logger.info(s"Batch size is $batchSize")

    val client = getClient
    val qb = termQuery("_type", fromIndexType)
    var sr = client.prepareSearch(fromIndexName)
      .setScroll(new TimeValue(timeValue))
      .setQuery(qb)
      .setSize(batchSize)
      .execute()
      .actionGet()

    var i = 0
    var documents = 0
    while(sr.hasNext) {

      i += 1
      val batch = sr.getBatchResponse
      documents += batch.length
      logger.info(s"Batch $i - reIndexing ${batch.length} documents")
      reImport(client, toIndexName, toIndexType, batch)
      sr = client.prepareSearchScroll(sr.getScrollId)
        .setScroll(new TimeValue(timeValue))
        .execute()
        .actionGet()
    }

    logger.info(s"$documents documents properly reindexed to $toIndexName/$toIndexType")
    client.close()

  }

  private def reImport(client: Client, indexName: String, indexType: String, batch: Array[(String, String)]) = {

    val bulkRequest = client.prepareBulk()
    batch.foreach({case (id, source) =>
      bulkRequest.add(client.prepareIndex(indexName, indexType, id).setSource(source))
    })

    val bulkResponse = bulkRequest.execute().actionGet()
    if (bulkResponse.hasFailures)
      throw new Exception(s"Failed to import batch - ${bulkResponse.buildFailureMessage()}")

  }

  private def getClient: Client = {
    val settings = Settings
      .settingsBuilder()
      .put("cluster.name", cluster)
      .put("shield.user", s"$username:$password")
      .build()

    val ta = new InetSocketTransportAddress(InetAddress.getByName(hostname), port)

    TransportClient
      .builder()
      .settings(settings)
      .build()
      .addTransportAddress(ta)
  }
}

object ReIndex extends LazyLogging {

  def main(args: Array[String]) = {

    if(args.length < 4) {
      logger.error("usage: <fromIndexName> <toIndexType> <toIndexName> <toIndexType>")
      System.exit(1)
    }

    val Array(fromIndexName, fromIndexType, toIndexName, toIndexType) = args.take(4)
    new ReIndex().process(fromIndexName, fromIndexType, toIndexName, toIndexType)

  }

  implicit class SearchResponse(sr: org.elasticsearch.action.search.SearchResponse) {

    def getBatchResponse: Array[(String, String)] = {
      sr.getHits.getHits.map({ hit =>
        (hit.getId, hit.getSourceAsString)
      })
    }

    def hasNext: Boolean = {
      sr.getHits.getHits.nonEmpty
    }

  }

}