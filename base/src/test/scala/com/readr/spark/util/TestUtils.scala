package com.readr.spark.util

import org.scalatest.{Matchers, FlatSpec}
  import org.scalatest._

  class UtilsSpec extends FlatSpec with Matchers {

    val s =
      """
This is a simple test.
Line-breaks should not be a deal here.
Everything works.
      """

    "escape" should "be unescapable" in {
      val esc = Utils.escape(s)
      val t = Utils.unescape(esc)
      assert(s == t)
    }

    "escape" should "not contain escaped chars" in {
      val esc = Utils.escape(s)
      assert(esc.indexOf('\n') == -1)
    }

}
