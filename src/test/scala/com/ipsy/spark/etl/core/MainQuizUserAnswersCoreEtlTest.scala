package com.ipsy.spark.etl.core

import java.util.regex.Pattern

import com.ipsy.data.schema.{ColumnDefinition, GenericColumn, GenericRow}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.jupiter.api.AfterAll
import org.junit.{Assert, Test}

import scala.collection.JavaConversions

import Vars._


class MainQuizUserAnswersCoreEtlTest {
  val sparkConf: SparkConf = new SparkConf()
    .setAppName("MainQuizUserAnswersCoreEtlTest")
    .setMaster("local[1]")
    .set("spark.ui.enabled", "false")

  val sc: SparkContext = SparkContext.getOrCreate(sparkConf)

  val pocUtils = new MainQuizUserAnswersCoreEtlPocUtils
  val columnList = pocUtils.getSchemaDefinition.getColumnDefinitions
  val columns = sc.broadcast(JavaConversions.asScalaBuffer(columnList).seq)
  val jsonPathV1 = Seq(
    UserPreferenceJsonPath("q-1", "$['q-1']", true),
    UserPreferenceJsonPath("q-2", "$['q-2']", false),
    UserPreferenceJsonPath("q-3", "$['q-3']", true)
  )
  val jsonPaths = sc.broadcast(Map("1.0" -> jsonPathV1))

  val idColumnDef = new ColumnDefinition("id", null, "character varying", null, false, 1, "main_quiz_user_answers")
  val userIdColumnDef = new ColumnDefinition("user_id", null, "character varying", null, false, 3, "main_quiz_user_answers")
  val preferencesColumnDef = new ColumnDefinition("preferences", null, "jsonb", null, false, 7, "main_quiz_user_answers")
  val jsonSchemaVersionColumnDef = new ColumnDefinition("json_schema_version", null, "character varying", null, false, 8, "main_quiz_user_answers")

  @AfterAll
  def tearDown(): Unit = {
    sc.stop()
  }

  @Test
  def testParseRows() = {
    val lines = Seq(
      """I|nqza-1111|0|u-1|2017-11-09 22:11:00.123456||2017-11-09 22:11:00.123456|"{test:1}"|1.0""",
      """I|nqza-2222|0|u-2|2017-11-09 22:22:00.654321||2017-11-09 22:22:00.654321|"{test:2}"|1.0""",
      """U|nqza-2222|1|u-2|2017-11-09 22:22:00.654321||2017-11-09 22:33:00.654321|"{test:3}"|1.0""",
      """I|nqza-3333|0|u-3|2017-11-09 22:44:00.111222||2017-11-09 22:44:00.111222|"{test:4}"|1.0""",
      """U|nqza-1111|1|u-1|2017-11-09 22:11:00.123456||2017-11-09 22:55:00.123456|"{test:5}"|1.0""",
      """U|nqza-2222|2|u-2|2017-11-09 22:22:00.654321||2017-11-09 22:33:00.654321|"{test:6}"|1.0"""
    )
    val expected = Seq(
      ("I", "u-1", 0, """nqza-1111|0|u-1|2017-11-09 22:11:00.123456||2017-11-09 22:11:00.123456|"{test:1}"|1.0"""),
      ("I", "u-2", 1, """nqza-2222|0|u-2|2017-11-09 22:22:00.654321||2017-11-09 22:22:00.654321|"{test:2}"|1.0"""),
      ("U", "u-2", 2, """nqza-2222|1|u-2|2017-11-09 22:22:00.654321||2017-11-09 22:33:00.654321|"{test:3}"|1.0"""),
      ("I", "u-3", 3, """nqza-3333|0|u-3|2017-11-09 22:44:00.111222||2017-11-09 22:44:00.111222|"{test:4}"|1.0"""),
      ("U", "u-1", 4, """nqza-1111|1|u-1|2017-11-09 22:11:00.123456||2017-11-09 22:55:00.123456|"{test:5}"|1.0"""),
      ("U", "u-2", 5, """nqza-2222|2|u-2|2017-11-09 22:22:00.654321||2017-11-09 22:33:00.654321|"{test:6}"|1.0""")
    )
    val rows = sc.parallelize(lines).zipWithIndex()
    val actual = MainQuizUserAnswersCoreEtl.parseRows(rows, columns, Pattern.quote("|")).collect().sortBy(_._3)
    for ((i, j) <- expected zip actual) {
      Assert.assertEquals(i._1, j._1)
      Assert.assertEquals(i._2, j._2)
      Assert.assertEquals(i._3, j._3)
      Assert.assertEquals(i._4, j._4.toString("|"))
    }
  }

  @Test
  def testGetAnswerUpdatedUser() = {
    val input = Seq(
      ("I", "u-1", 0l, new GenericRow()),
      ("I", "u-2", 1l, new GenericRow()),
      ("U", "u-2", 2l, new GenericRow()),
      ("I", "u-3", 3l, new GenericRow()),
      ("U", "u-4", 4l, new GenericRow()),
      ("U", "u-1", 5l, new GenericRow()),
      ("U", "u-2", 6l, new GenericRow())
    )
    val rows = sc.parallelize(input)
    val actual = MainQuizUserAnswersCoreEtl.getAnswerUpdatedUser(rows).collect().sorted
    Assert.assertEquals(3, actual.length)
    Assert.assertEquals("u-1", actual(0))
    Assert.assertEquals("u-2", actual(1))
    Assert.assertEquals("u-4", actual(2))
  }

  @Test
  def testAddIsValidColumn() = {
    val input = Seq(
      ("u-1", 0l, new GenericRow()),
      ("u-2", 1l, new GenericRow()),
      ("u-2", 2l, new GenericRow()),
      ("u-3", 3l, new GenericRow()),
      ("u-4", 4l, new GenericRow()),
      ("u-1", 5l, new GenericRow()),
      ("u-2", 6l, new GenericRow())
    )
    input.map(row => row._3.addColumn(new GenericColumn(idColumnDef, row._2.toString)))
    val expected = Seq("f,0", "f,1", "f,2", "t,3", "t,4", "t,5", "t,6")
    val rows = sc.parallelize(input)
    val actual = MainQuizUserAnswersCoreEtl.addIsValidColumn(rows).collect().sortBy(_.getColumn("id").getValue)
    for ((i, j) <- expected zip actual) {
      Assert.assertEquals(i, j.toString)
    }
  }

  @Test
  def testNormalizePreferences() = {
    val lines = Seq(
      ("u-1","f",""""{""q-1"": [""q1u1a1""], ""q-2"": "q2u1a1", ""q-3"": [""q3u1a1"",""q3u1a2"",""q3u1a3""]}""""),
      ("u-2","t",""""{""q-1"": [""q1u2a1"",""q1u2a2""], ""q-2"": "q2u2a1", ""q-3"": [""q3u2a1"",""q3u2a2""]}""""),
      ("u-1","t",""""{""q-1"": [""q1u1a11""], ""q-2"": "q2u1a11", ""q-3"": [""q3u1a11""]}"""")
    )
    val rowSeq = lines.map(line => {
      val row = new GenericRow()
      row.addColumn(new GenericColumn(jsonSchemaVersionColumnDef, "1.0"))
      row.addColumn(new GenericColumn(userIdColumnDef, line._1))
      row.addColumn(new GenericColumn(isValidColumnDefinition, line._2))
      row.addColumn(new GenericColumn(preferencesColumnDef, line._3))
      row
    })
    val rows = sc.parallelize(rowSeq)
    val actual = MainQuizUserAnswersCoreEtl.normalizePreferences(rows, jsonPaths).collect()
      .sortBy(r => (r.getColumn("user_id").getValue, r.getColumn("is_valid_now").getValue,
        r.getColumn("quiz_question").getValue, r.getColumn("quiz_answer").getValue))
    val expected = Seq(
      "q-1,q1u1a1,f,u-1,1.0",
      "q-2,q2u1a1,f,u-1,1.0",
      "q-3,q3u1a1,f,u-1,1.0",
      "q-3,q3u1a2,f,u-1,1.0",
      "q-3,q3u1a3,f,u-1,1.0",
      "q-1,q1u1a11,t,u-1,1.0",
      "q-2,q2u1a11,t,u-1,1.0",
      "q-3,q3u1a11,t,u-1,1.0",
      "q-1,q1u2a1,t,u-2,1.0",
      "q-1,q1u2a2,t,u-2,1.0",
      "q-2,q2u2a1,t,u-2,1.0",
      "q-3,q3u2a1,t,u-2,1.0",
      "q-3,q3u2a2,t,u-2,1.0"
    )
    for ((i, j) <- expected zip actual) {
      Assert.assertEquals(i, j.toString)
    }
  }
}
