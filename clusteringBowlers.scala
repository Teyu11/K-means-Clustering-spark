import org.apache.spark.mllib.linalg.Vectors
import au.com.bytecode.opencsv.CSVWriter
import java.io.FileWriter
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.sql.functions._

//Load the bowling averages file
val data = sc.textFile("hdfs://localhost:54310/bowling.csv")

//Define the Case class to be able to create objects of the class later on
case class CC1(Player: String, Team: String, Mat: Double, Inns: Double, Overs: Double,Maidens:Double, Runs: Double, Wkts:Double,BBI:String, Ave: Double, Econ: Double, SR: Double,N4:Double,N5:Double,Ct:Double,St:Double)



//Comma Seperator split
val allSplit = data.map(line => line.split(","))


//Map the values obtained after the split to the Case class
val allData = allSplit.map( p => CC1( p(0).toString, p(1).toString, p(2).trim.toDouble, p(3).trim.toDouble, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim.toString, p(9).trim.toDouble, p(10).trim.toDouble, p(11).trim.toDouble, p(12).trim.toDouble, p(13).trim.toDouble, p(14).trim.toDouble,p(15).trim.toDouble))

//allData.foreach(println)


//Convert the new RDD back into a dataframe format

val backTodf = allData.toDF()
//backTodf.show()


//Convert back to RDD and cache data for faster recovery 
val allValRDDbo = backTodf.rdd.map(r => (r.getString(0), r.getString(1), r.getDouble(2), r.getDouble(3), r.getDouble(4), r.getDouble(5),r.getDouble(6), r.getDouble(7), r.getString(8), r.getDouble(9), r.getDouble(10), r.getDouble(11), r.getDouble(12), r.getDouble(13), r.getDouble(14),r.getDouble(15) ))

allValRDDbo.cache()


//Now, convert the dataframe to the RDD which will contain the parameters to be used for kmeans clustering. Cache this data since it will be used in frequent iterations. We are passing Maiden Overs (No of overs in which the bowler conceded 0 runs), Bowling Average (Avg number of runs conceded per wicket), Strike Rate(Avg number of balls bowled per wicket taken), Economy Rate (Avg no of runs conceded per over)
val paramRDDbo = backTodf.rdd.map(r => Vectors.dense( r.getDouble(5), r.getDouble(10),r.getDouble(11)))

paramRDDbo.cache()

//We now need to figure out optimal k

val clusters1 = KMeans.train(paramRDDbo,1,100)
val WSSSE1 = clusters1.computeCost(paramRDDbo)
println("Within Set Sum of Squared Errors = " + WSSSE1) 

val clusters2 = KMeans.train(paramRDDbo,2,100)
val WSSSE2 = clusters2.computeCost(paramRDDbo)
println("Within Set Sum of Squared Errors = " + WSSSE2) 

val clusters3 = KMeans.train(paramRDDbo,3,100)
val WSSSE3 = clusters3.computeCost(paramRDDbo)
println("Within Set Sum of Squared Errors = " + WSSSE3) 

val clusters4 = KMeans.train(paramRDDbo,4,100)
val WSSSE4 = clusters4.computeCost(paramRDDbo)
println("Within Set Sum of Squared Errors = " + WSSSE4) 

val clusters5 = KMeans.train(paramRDDbo,5,100)
val WSSSE5 = clusters5.computeCost(paramRDDbo)
println("Within Set Sum of Squared Errors = " + WSSSE5) 

val clusters6 = KMeans.train(paramRDDbo,6,100)
val WSSSE6 = clusters6.computeCost(paramRDDbo)
println("Within Set Sum of Squared Errors = " + WSSSE6) 

val clusters7 = KMeans.train(paramRDDbo,7,100)
val WSSSE7 = clusters7.computeCost(paramRDDbo)
println("Within Set Sum of Squared Errors = " + WSSSE7) 

val clusters8 = KMeans.train(paramRDDbo,8,100)
val WSSSE8 = clusters8.computeCost(paramRDDbo)
println("Within Set Sum of Squared Errors = " + WSSSE8) 

val clusters9 = KMeans.train(paramRDDbo,9,100)
val WSSSE9 = clusters9.computeCost(paramRDDbo)
println("Within Set Sum of Squared Errors = " + WSSSE9) 

val clusters10 = KMeans.train(paramRDDbo,10,100)
val WSSSE10 = clusters10.computeCost(paramRDDbo)
println("Within Set Sum of Squared Errors = " + WSSSE10) 

val clusters11 = KMeans.train(paramRDDbo,11,100)
val WSSSE11 = clusters11.computeCost(paramRDDbo)
println("Within Set Sum of Squared Errors = " + WSSSE11) 

val clusters12 = KMeans.train(paramRDDbo,12,100)
val WSSSE12 = clusters12.computeCost(paramRDDbo)
println("Within Set Sum of Squared Errors = " + WSSSE12) 

val clusters13 = KMeans.train(paramRDDbo,13,100)
val WSSSE13 = clusters13.computeCost(paramRDDbo)
println("Within Set Sum of Squared Errors = " + WSSSE13) 

val clusters14 = KMeans.train(paramRDDbo,14,100)
val WSSSE14 = clusters14.computeCost(paramRDDbo)
println("Within Set Sum of Squared Errors = " + WSSSE14) 

val clusters15 = KMeans.train(paramRDDbo,15,100)
val WSSSE15 = clusters15.computeCost(paramRDDbo)
println("Within Set Sum of Squared Errors = " + WSSSE15) 

val clusters16 = KMeans.train(paramRDDbo,16,100)
val WSSSE16 = clusters16.computeCost(paramRDDbo)
println("Within Set Sum of Squared Errors = " + WSSSE16) 

val clusters17 = KMeans.train(paramRDDbo,17,100)
val WSSSE17 = clusters17.computeCost(paramRDDbo)
println("Within Set Sum of Squared Errors = " + WSSSE17) 

val clusters18 = KMeans.train(paramRDDbo,18,100)
val WSSSE18 = clusters18.computeCost(paramRDDbo)
println("Within Set Sum of Squared Errors = " + WSSSE18) 

val clusters19 = KMeans.train(paramRDDbo,19,100)
val WSSSE19 = clusters19.computeCost(paramRDDbo)
println("Within Set Sum of Squared Errors = " + WSSSE19) 

val clusters20 = KMeans.train(paramRDDbo,20,100)
val WSSSE20 = clusters20.computeCost(paramRDDbo)
println("Within Set Sum of Squared Errors = " + WSSSE20) 


//After plotting the Elbow Curve in Libre Office Calc, we take k to be 4
//Therefore we further use clusters4

//Print the centroid of each cluster
println("Displaying the centroids of each of the clusters")
clusters4.clusterCenters.foreach(println)


//Get the prediction from the model with the ID so we can link them back to other information
val predictions = allValRDDbo.map{r => (r._1, clusters4.predict(Vectors.dense(r._6,r._11,r._12) ))}

//Convert the rdd to a dataframe
val finalDF = predictions.toDF("Player", "CLUSTER")

//finalDF.show()

finalDF.write.format("com.databricks.spark.csv").option("header","true").save("tatakai.csv")

paramRDDbo.unpersist()
allValRDDbo.unpersist()



