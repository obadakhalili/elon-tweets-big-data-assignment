import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ElonTweets {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(ElonTweets.getClass.getName)
    val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()
    val sc = spark.sparkContext

    val tweetsRows = sc.textFile("./tweets.csv")

    val header = tweetsRows.first()

    // the below code returns a list of tweets, where each tweet is a tuple of (date, [tokens])
    val tweets = tweetsRows
      .filter(tweet => tweet != header)
      .map(tweet => {
        val parserRegex = ".+,(\\d{4}-\\d{2}-\\d{2}).*,(?:(?:\"?b'(.+)'\"?)|(?:\"b\"\"(.+)\"\"\"))".r
        val parserRegex(date, tweet1, tweet2) = tweet
        val tweetText = if (tweet1 != null) tweet1 else tweet2
        val tweetTokens = tweetText
          .toLowerCase()
          .replaceAll("[,\\'\\?\\.\\!\\(\\)\\:\\\"]+", "")
          .split(" ")
          .filter(tokens => tokens.matches("[a-zA-Z\\-]+"))
        
        (date, tweetTokens)
      })

    print("Please enter a comma-separated list of tokens: ")
    val userTokens = scala.io.StdIn.readLine().split(",")
    
    // requirement 1
    // the code below is the implementation for my old understanding of the requirement, where I return
    // the count of tokens mentioned in all tweets published in a given date.
    val tweetsTokensCountByDate = tweets
      .reduceByKey((tweet1, tweet2) => tweet1 ++ tweet2)
      .mapValues((tweetsTokens) => {
        val tweetsTokensCount = tweetsTokens
          .groupBy(identity)
          .mapValues(_.size)

        tweetsTokensCount
      })
    
    // the below code extracts the answer for the tokens that the user entered.
    val userTokensCountByDate = tweetsTokensCountByDate
      .filter((tweetsTokensCount) => {
        tweetsTokensCount._2.exists((tweetTokenCount) => {
          userTokens.contains(tweetTokenCount._1)
        })
      })
      .mapValues((userTokensCount) => {
        userTokensCount.filter((userTokenCount) => {
          userTokens.contains(userTokenCount._1)
        })
      })
    
    // validate requirement 1:
    userTokensCountByDate.foreach((userTokensCount) => {
      println("Tokens count of tweets published on " + userTokensCount._1 + ":")
      userTokensCount._2.foreach((userTokenCount) => {
        println("  " + userTokenCount._1 + ": " + userTokenCount._2)
      })
    })
    
    // requirement 2 (old).
    // This is the solution for my old understanding of requirement 2, where I return
    // the count of tweets mentioning every token in the corpus. Unfortunately, I can't use it to extract
    // a solution for only the user-entered tokens like I did in requirement 1, but I thought it's worth
    // keeping it here.
    val tokensTweetsCount = tweets
      .mapValues((tweetTokens) => {
        val uniqueTweetTokens = tweetTokens
          .groupBy(identity)
          .mapValues(_ => 1)

        uniqueTweetTokens
      })
      .flatMap((tweet) => tweet._2)
      .reduceByKey((occurrence1, occurrence2) => (occurrence1 + occurrence2))
    
    // validate requirement 2 (old):
    // println("Tokens count in all tweets:")
    // tokensTweetsCount.foreach((token) => {
    //   println("  " + token._1 + ": " + token._2)
    // })

    // requirement 2 (new)
    // the below code returns the count of tweets mentioning at least one of the user entered tokens.
    val tweetsContainingUserTokensCount = tweets.filter((tweet) => {
      tweet._2.exists((tweetToken) => {
        userTokens.contains(tweetToken)
      })
    })
    .count()
    .toDouble
    val tweetsCount = tweets.count().toDouble
    val tweetsContainingUserTokensPercentage = tweetsContainingUserTokensCount / tweetsCount * 100

    // validate requirement 2 (new)
    println("Percentage of tweets mentioning at least one of the user tokens: " + tweetsContainingUserTokensPercentage + "%")

    // requirement 3
    // the code below returns the count of tweets mentioning exactly any 2 of the user entered tokens.
    val tweetsCountMentioningTwoUserTokens = tweets
      .filter((tweet) => {
        val tweetTokens = tweet._2
        val mentionedUserTokensCount = tweetTokens
          .distinct
          .map((token) => {
            if (userTokens.contains(token)) 1 else 0
          })
          .sum
        
        mentionedUserTokensCount == 2
      })
      .count()
      .toDouble
    val tweetsPercentageMentioningTwoUserTokens = tweetsCountMentioningTwoUserTokens / tweetsCount * 100

    // validate requirement 3
    println("Percentage of tweets mentioning exactly any 2 of the user tokens: " + tweetsPercentageMentioningTwoUserTokens + "%")

    // requirement 4
    val tweetsLengthStats = tweets
      .mapValues((tweetTokens) => tweetTokens.length)
      .map((tweet) => tweet._2)
      .stats()

    // validate requirement 4
    println("Average tweet length: " + "%.2f".format(tweetsLengthStats.mean))
    println("Standard deviation of tweet length: " + "%.2f".format(tweetsLengthStats.stdev))

    spark.stop()
  }
}

