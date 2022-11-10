package za.co.twitter

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter._
import org.slf4j.LoggerFactory
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import java.util.Properties
/**
 * @author ${user.name}
 */

object Twitter extends App{
    LoggerFactory.getLogger(this.getClass).atError()

    val properties =  new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("./application.properties"))
    val sparkConf = new SparkConf().setAppName("TwitterInsights").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val cb = new ConfigurationBuilder
        cb.setDebugEnabled(true).setOAuthConsumerKey(properties.getProperty("twitter4j.oauth.consumerKey"))
          .setOAuthConsumerSecret(properties.getProperty("twitter4j.oauth.consumerSecret"))
          .setOAuthAccessToken(properties.getProperty("twitter4j.oauth.accessToken"))
          .setOAuthAccessTokenSecret(properties.getProperty("twitter4j.oauth.accessTokenSecret"))

    val auth = new OAuthAuthorization(cb.build)
    val tweets = TwitterUtils.createStream(ssc, Some(auth), Array("programming,computer science,Big data,hadoop,kafka,spark,aws"))

    val noOfHashtagPerUser : DStream[(String,String, Int)] = tweets.filter(tweet => tweet.getLang.equals("en"))
                    .map(tweet => (tweet.getUser.getName,tweet.getText))
                    .map(tweet => (tweet._1,tweet._2.split(' ').toList))
                    .mapValues(tweetList => tweetList.filter(tweetWord => tweetWord.startsWith("#")))
                    .mapValues(tweetList => tweetList.map(tweetWord => tweetWord.trim.toLowerCase))
                    .filter(tweetWord => tweetWord._2.nonEmpty)
                    .flatMapValues( tweetList => tweetList)
      // Count each hashtag in each batch
                    .map(tweetInsight => (tweetInsight._1,tweetInsight._2,1))
      //group by username and get number of hashtags they tweeted
                    .reduce((tweetInsight1, tweetInsight2) =>
                        (tweetInsight1._1, s"${tweetInsight1._2}|${tweetInsight2._2}",tweetInsight1._3 + tweetInsight2._3))

    noOfHashtagPerUser.print(100)

    noOfHashtagPerUser.saveAsTextFiles("src/main/results/TrendingHashTagsFromPastHour")

    ssc.start()
    ssc.awaitTermination()
}
