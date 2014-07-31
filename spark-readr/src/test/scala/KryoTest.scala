import org.apache.spark.SparkContext._
import com.readr.model._
import com.readr.model.annotation._
import org.apache.spark._
import com.esotericsoftware.kryo._
import com.esotericsoftware.kryo.io._
import org.apache.spark.serializer._
import java.io._
import com.readr.model.Offsets

import java.util.ArrayList
import com.twitter.chill.ScalaKryoInstantiator

import scala.collection.immutable.Seq

// an assembly over everything (?)

//class App extends ProcessModule {
//  // set annotator
//  def init() = {
//    //setProcessorAnnotator(classOf[StanfordTokenAnnotator])
//  }  
//}

class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Offsets])
  }
}

class Person(val name: String, val age: Int)

object KryoTest {
  
  def main(args:Array[String]) = {
//    val conf = new SparkConf().setMaster("local").setAppName("test")
//    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    conf.set("spark.kryo.registrator", "com.readr.MyRegistrator")
//
//    val sc = new SparkContext(conf)
    
//    val people = Array(new Person("Alex", 20), new Person("Alex", 20), new Person("Alex", 20), new Person("Alex", 20), new Person("Alex", 20), new Person("Alex", 20), new Person("Alex", 20), new Person("Alex", 20), new Person("Alex", 20), new Person("Alex", 20), new Person("Alex", 20), new Person("Alex", 20), new Person("Barbara", 25), new Person("Charles", 30), 
//        new Person("David", 35), new Person("Emily", 40))
//        
//    val kryo = new Kryo
//    //kryo.setRegistrationRequired(false)
//    kryo.register(classOf[Person])
//    
//    for (i <- 0 until 10000) {
//      val output = new Output(new ByteArrayOutputStream)      
//      kryo.writeObject(output, people)
//      if (i == 0) println(output.total())
//    }
//    val startTime = System.nanoTime
//    for (i <- 0 until 10000) {
//      val output = new Output(new ByteArrayOutputStream)      
//      kryo.writeObject(output, people)
//    }
//    println((System.nanoTime - startTime) / 1e9)
    
    testKryo()
  //  testProtobuf()
  }
  
//  case class Token(value:String, offsetBegin:Int, offsetEnd:Int, original:String) {}
//  case class TokenDoc(id:Int, tokens:Array[Token])
//  val td = TokenDoc(43, Array(Token("ja", 24, 25, "ja"), Token("ja", 24, 25, "ja"), Token("ja", 24, 25, "ja"), Token("ja", 24, 25, "ja"), Token("ja", 24, 25, "ja"), Token("ja", 24, 25, "ja"), Token("ja", 24, 25, "ja"), Token("ja", 24, 25, "ja"), Token("ja", 24, 25, "ja"), Token("ja", 24, 25, "ja"), Token("ja", 24, 25, "ja"), Token("ja", 24, 25, "ja"), Token("ja", 24, 25, "ja"), Token("du", 83, 84, "du")))

  case class Token(offsetBegin:Int = -1, offsetEnd:Int = -1) {}
  case class TokenDoc(id:Int = -1, tokens:Any = Nil) {
    //def this() {  this(-1, Nil)} 
  }
  val td = TokenDoc(43, Array(Tuple2(1,Array(Token(24, 25), Token(26, 25), Token(31, 25), Token(24, 28), Token(124, 25), Token(24, 254), Token(2224, 25), Token(22224, 25), Token(24, 33325), Token(24, 2522)))))
  //val td = TokenDoc(43, Array(Tuple2(1,Seq(Token(24, 25), Token(26, 25), Token(31, 25), Token(24, 28), Token(124, 25), Token(24, 254), Token(2224, 25), Token(22224, 25), Token(24, 33325), Token(24, 2522)))))
//      
//      , Token("ja", 24, 25, "ja"), Token("ja", 24, 25, "ja"), Token("ja", 24, 25, "ja"), Token("du", 83, 84, "du")))
//
  def testKryo() = {
//    val kryo = new Kryo
//    kryo.register(classOf[TokenDoc])
//    kryo.register(classOf[Token])
//    kryo.register(classOf[scala.Tuple2[Int,Array[Token]]])
//    
//    val output = new Output(new ByteArrayOutputStream)      
//    kryo.writeObject(output, td)
//    println(output.toBytes.length)
//    
//    val input = new Input(new ByteArrayInputStream(output.toBytes))
//    val ntd = kryo.readObject(input, classOf[TokenDoc])
//    println(ntd)
//    
    
    val instantiator = new ScalaKryoInstantiator
    instantiator.setRegistrationRequired(false)
    
    val kryo = instantiator.newKryo()
    
    kryo.register(classOf[TokenDoc])
    kryo.register(classOf[Token])
    kryo.register(classOf[scala.Tuple2[Int,Seq[Token]]])
    //kryo.register(classOf[scala.collection.immutable.Seq[scala.Tuple2[Int,Seq[Token]]]])
    
    
    val baos = new ByteArrayOutputStream
    val output = new Output(baos, 4096)
    kryo.writeObject(output, td)
    println(baos.size)
 
    val input = new Input(baos.toByteArray)
    val deser = kryo.readObject(input, classOf[TokenDoc])
    println(deser)
    
  }
  
  
//  def testProtobuf() = {
//    
//    val li = new ArrayList[TokenProto.Token](td.tokens.size)
//	for (t:Token <- td.tokens) {
//	  val nt = TokenProto.Token.newBuilder().
//			  //setValue(t.value).
//			  setOffsetBegin(t.offsetBegin).
//			  setOffsetEnd(t.offsetEnd).
//			  //setOriginal(t.original).
//			  build()
//	  li.add(nt);
//	}
//		
//	val tp = TokenProto.Doc.newBuilder().addAllTokens(li).setId(td.id).build()
//	val ba = tp.toByteArray()
//	println(ba.length)
//	//return tp;
//  }
  
}
