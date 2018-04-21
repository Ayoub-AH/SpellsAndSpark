package com.bddr.spark

import scala.collection.mutable.ArrayBuffer

class Creature (val name : String) extends Serializable {
 
   var spells =  Array[String]()
   var allSpells : String = "" 
   
  def addspell(spell : String) : Unit = {
      spells = spells :+ spell
      
      if(allSpells == ""){
           allSpells += spell.toString()
      }
      else{
          allSpells += ", "+ spell.toString()
      }
   }
   
   def spellExist(spell : String) : Boolean  = {
     return spells.contains(spell)
   }
   
   def printCreature() {
     println(this.name +" : " + allSpells)
   }

}