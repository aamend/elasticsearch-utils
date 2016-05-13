package com.aamend.elasticsearch

import java.io.{FileOutputStream, File}
import java.net.InetAddress

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilders._
import ESUtils.SearchResponse
import org.elasticsearch.shield.ShieldPlugin
import org.elasticsearch.shield.authc.support.SecuredString
import org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue

/**
  * Created by antoine on 10/05/2016.
  */
class ESUtils extends LazyLogging {

  val config = ConfigFactory.load()
  val hostname = config.getString("host")
  val port = config.getInt("port")
  val cluster = config.getString("cluster")
  val timeValue = config.getInt("timeValue")
  val batchSize = config.getInt("batchSize")
  val username = config.getString("username")
  val password = config.getString("password")

  val token = basicAuthHeaderValue(username, new SecuredString(password.toCharArray))

  def process(fromIndexName: String, fromIndexType: String, write: Boolean, toIndexName: Option[String], toIndexType: Option[String], outputFile: Option[String]) = {

    logger.info(s"Exporting data from $fromIndexName/$fromIndexType")
    logger.info(s"Batch size is $batchSize")

    if(write)
      logger.info(s"Importing data to $toIndexName/$toIndexType")

    if(outputFile.isDefined)
      logger.info(s"Exporting data to ${outputFile.get}")

    val client = getClient

    val qb = termQuery("_type", fromIndexType)
    var sr = client.prepareSearch(fromIndexName)
      .putHeader("Authorization", token)
      .setScroll(new TimeValue(timeValue))
      .setQuery(qb)
      .setSize(batchSize)
      .execute()
      .actionGet()

    val total = sr.getHits.getTotalHits

    var i = 0
    var documents = 0
    while(sr.hasNext) {

      i += 1
      val batch = sr.getBatchResponse
      documents += batch.length
      logger.info(s"Batch $i - $documents/$total")

      if(write && toIndexName.isDefined && toIndexType.isDefined)
        reImport(client, toIndexName.get, toIndexType.get, batch)

      if(outputFile.isDefined)
        appendToFile(outputFile.get, batch)

      sr = client.prepareSearchScroll(sr.getScrollId)
        .putHeader("Authorization", token)
        .setScroll(new TimeValue(timeValue))
        .execute()
        .actionGet()
    }

    logger.info(s"$documents documents properly processed")
    client.close()

  }

  private def appendToFile(outputFile: String,  batch: Array[(String, String)]) = {

    def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
      val p = new java.io.PrintWriter(new FileOutputStream(new File(outputFile), true))
      try { op(p) } finally { p.close() }
    }

    printToFile(new File(outputFile)) { p =>
      batch foreach { case (id, source) =>
        val json = "{\"index\":{\"_id\": \"" + id + "\"}}"
        p.println(json)
        p.println(source)
      }
    }
  }

  private def reImport(client: Client, indexName: String, indexType: String, batch: Array[(String, String)]) = {

    val bulkRequest = client.prepareBulk().putHeader("Authorization", token)
    batch foreach { case (id, source) =>
      bulkRequest.add(client.prepareIndex(indexName, indexType, id).setSource(source))
    }

    val bulkResponse = bulkRequest.execute().actionGet()
    if (bulkResponse.hasFailures)
      throw new Exception(s"Failed to import batch - ${bulkResponse.buildFailureMessage()}")

  }

  private def getClient: Client = {

    val settings = Settings.settingsBuilder()
      .put("cluster.name", cluster)
      .put("shield.user", s"$username:$password")
      .build()

    val ta = new InetSocketTransportAddress(InetAddress.getByName(hostname), port)

    TransportClient.builder()
      .addPlugin(classOf[ShieldPlugin])
      .settings(settings)
      .build()
      .addTransportAddress(ta)
  }
}

object ESUtils extends LazyLogging {

  implicit class SearchResponse(sr: org.elasticsearch.action.search.SearchResponse) {

    def getBatchResponse: Array[(String, String)] = {
      sr.getHits.getHits map { hit =>
        (hit.getId, hit.getSourceAsString)
      }
    }

    def hasNext: Boolean = {
      sr.getHits.getHits.nonEmpty
    }

  }

}