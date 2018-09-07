package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
    */
  def balance(chars: Array[Char]): Boolean = {
    balanceAcc(chars, 0, 0)
  }

  @tailrec
  def balanceAcc(chars: Array[Char], idx: Int, acc: Int): Boolean ={
    if(acc < 0)
      false
    else {
      if(idx < chars.length) {
        if (chars(idx) == '(')
          balanceAcc(chars, idx + 1, acc + 1)
        else if (chars(idx) == ')')
          balanceAcc(chars, idx + 1, acc - 1)
        else
          balanceAcc(chars, idx + 1, acc)
      }else{
        acc == 0
      }
    }
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
    */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, closed: Int, open: Int) : (Int, Int) = {
      if(idx >= until){
        (closed, open)
      }else {
        val c = chars(idx)
        if (c == '(') {
          traverse(idx + 1, until, closed, open + 1)
        } else if (c == ')') {
          if (open > 0)
            traverse(idx + 1, until, closed, open - 1)
          else
            traverse(idx + 1, until, closed - 1, open)
        }
        else {
          traverse(idx + 1, until, closed, open)
        }
      }
    }


    def reduce(from: Int, until: Int) : (Int, Int) = {
      val numElem = until-from
      if(numElem > threshold){
        val ((closed1, open1),(closed2, open2)) =
          parallel( reduce(from, from+numElem/2), reduce(from+numElem/2, until) )
        val dif = open1 + closed2
        if(dif<0){
          (closed1+dif, open2)
        }else{
          (closed1, open2+dif)
        }
      }else{
        traverse(from, until, 0, 0)
      }
    }
    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
