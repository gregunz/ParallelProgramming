package reductions

import java.util.concurrent._
import scala.collection._
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import common._
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory

@RunWith(classOf[JUnitRunner]) 
class LineOfSightSuite extends FunSuite {
  import LineOfSight._
  test("lineOfSight should correctly handle an array of size 4") {
    val output = new Array[Float](4)
    lineOfSight(Array[Float](0f, 1f, 8f, 9f), output)
    assert(output.toList == List(0f, 1f, 4f, 4f))
  }


  test("upsweepSequential should correctly handle the chunk 1 until 4 of an array of 4 elements") {
    val res = upsweepSequential(Array[Float](0f, 1f, 8f, 9f), 1, 4)
    assert(res == 4f)
  }


  test("downsweepSequential should correctly handle a 4 element array when the starting angle is zero") {
    val output = new Array[Float](4)
    downsweepSequential(Array[Float](0f, 1f, 8f, 9f), output, 0f, 1, 4)
    assert(output.toList == List(0f, 1f, 4f, 4f))
  }
  test("BIG TEST") {
    val randomF = scala.util.Random.nextFloat
    val length = 5000
    val input = (0 until length).map(i => i % 100 * 1.0f + randomF*10).toArray
    val output1 = new Array[Float](length + 1)
    val output2 = new Array[Float](length + 1)
    lineOfSight(input, output1)
    parLineOfSight(input, output2, 1000)
    assert ( output2.toList == output1.toList )
  }

  /*test("parallel BIG TEST") {
    val length = 150
    val input = (0 until length).map(_ % 100 * 1.0f).toArray
    val output = new Array[Float](length + 1)
    parLineOfSight(input, output, 10000)
    assert(output.toList.length == (0f :: (0 until length).map(_ => 1f).toList).length )
    //assert(output.toList == (0f :: (0 until length-1).map(_ => 1f).toList) )
  }*/

  test("PARALLEL lineOfSight should correctly handle an array of size 4") {
    val output = new Array[Float](4)
    parLineOfSight(Array[Float](0f, 1f, 8f, 9f), output, 2)
    assert(output.toList == List(0f, 1f, 4f, 4f))
  }

  test("downsweep1") {
    val input: Array[Float] = List(0.0f, 7.0f, 9f, 11.0f*3f, 12.0f*4f).toArray
    val output1 = new Array[Float](input.length)
    val output2 = new Array[Float](input.length)
    lineOfSight(input, output1)
    parLineOfSight(input, output2, 2)
    assert ( output2.toList == output1.toList )
  }
  test("downsweep2") {
    val input: Array[Float] = List(0.0f, 7.0f, 9f, 11.0f*3f, 12.0f*4f).toArray
    val output1 = new Array[Float](input.length)
    val output3 = new Array[Float](input.length)
    lineOfSight(input, output1)
    parLineOfSight(input, output3, 1000)
    assert ( output3.toList == output1.toList )
  }
  test("downsweep count parallel") {
    /*countD = 0
    countU = 0*/
    val list: List[Float] = List(0.0f, 7.0f, 9f, 11.0f*3f, 12.0f*4f)
    val input = (0.0f :: 0.0f :: (list ++ list ++ list)).toArray
    //println(input.length)
    val output1 = new Array[Float](input.length)
    val output3 = new Array[Float](input.length)
    lineOfSight(input, output1)
    parLineOfSight(input, output3, 1)
    assert ( output3.toList == output1.toList )
    /*println()
    println(countU, countU)
    println(output3.length)*/
  }
  val input0 = (0 to math.pow(10, 6).toInt).map(_ % 100 + 0f).toArray
  test("not parallel"){
    val output = new Array[Float](input0.length)
    lineOfSight(input0, output)
  }

  test("parallel"){
    val output = new Array[Float](input0.length)
    parLineOfSight(input0, output, math.pow(10, 5).toInt)
  }

}

