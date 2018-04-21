package com.bddr.spark

import data.Crawling
import scala.io.Source
import java.io.File
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.collection.mutable


object Main {
  var creatures : Array[Creature] = Array()
  
   def main(args: Array[String]): Unit = {
     
     if(!(new File("spells")).exists()){
       val data =new Crawling("")
	     data.getMonsters()
     }
     
	   spellsInObject()
	   
	   //Start the Spark context
    val conf = new SparkConf()
      .setAppName("SparkAndSpells")
      .setMaster("local")
    val sc = new SparkContext(conf)
	   
	    var allSpells : Array[String] = Array()
	  for(creature <- creatures){
     for(spell <- creature.spells){
       var name = creature.name
       if(name.contains(",")){
         name = name.replace(",", ":")
       }
       allSpells = allSpells :+ (spell +"="+ name)
     }
	  }
	   val data = sc.parallelize(allSpells)
	   //Create key value pairs
    val kv = data.map(_.split("=")).map(v => (v(0), v(1))).cache()

    val initialSet = mutable.HashSet.empty[String]
    val addToSet = (s: mutable.HashSet[String], v: String) => s += v
    val mergePartitionSets = (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2

    val uniqueByKey = kv.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
    
    uniqueByKey.foreach(println)
    }
   
   def spellsInObject (){
	   
	   val listFiles = (new File("spells")).listFiles.filter(_.getName.endsWith(".txt"))
	   
	   var index = 0
      for(file <- listFiles){
        val filename = file.getName
        
        val creature : Creature = new Creature(filename.substring(0, filename.length()-4))
        creatures = creatures :+ creature
        for (spell <- Source.fromFile("spells/"+filename).getLines) {
          
          if(spell != "\n" && spell != "" && !creatures(index).spellExist(spell)){
            creatures(index).addspell(spell)
          }  
        }
        //creatures(index).printCreature()
        index += 1
      }
   }
}