package com.ipsy.spark.etl.core

import java.util.regex.Pattern

import com.ipsy.data.schema.{ColumnDefinition, GenericColumn, GenericRow}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json.{JSONException, JSONObject}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.{JavaConversions, mutable}

object MainQuizUserAnswersCoreEtlVars {
  val logger: Logger = LoggerFactory.getLogger(MainQuizUserAnswersCoreEtl.getClass)
  val isValidColumnDefinition = new ColumnDefinition("is_valid_now", null, "boolean", "t", false, 0, "public.main_quiz_user_answers")
  val quizQuestionColumnDefinition = new ColumnDefinition("quiz_question", 255, "character varying", null, false, -2, "ipsy.quiz_user_answers")
  val quizAnswerColumnDefinition = new ColumnDefinition("quiz_answer", 255, "character varying", null, false, -1, "ipsy.quiz_user_answers")
}

object MainQuizUserAnswersCoreEtl extends App {
  def parseRows(rowsWithIndex: RDD[(String, Long)], columnsBroadcast: Broadcast[mutable.Seq[ColumnDefinition]], delimiter: String): RDD[(String, String, Long, GenericRow)] = {
    val columnDefinitions = columnsBroadcast.value
    rowsWithIndex.map(row => {
      val fields = row._1.split(delimiter)
      val genericRow = new GenericRow
      for (columnDefinition <- columnDefinitions) {
        val index = columnDefinition.getOrdinalPosition
        if (index >= fields.length) {
          throw new IndexOutOfBoundsException(s"Data has ${fields.length - 1} columns while definition is ${columnDefinition.getColumnName} at $index")
        }
        genericRow.addColumn(new GenericColumn(columnDefinition, fields(index)))
      }
      // (update_flag, user_id, line_number, row)
      (fields.head, genericRow.getColumn("user_id").getValue, row._2, genericRow)
    })

  }

  def addIsValidColumn(rows: RDD[(String, Long, GenericRow)]): RDD[GenericRow] = {
    import MainQuizUserAnswersCoreEtlVars._
    val groupedRows = rows.groupBy(_._1).mapValues(userPreferences => {
      val sortedPreferences = userPreferences.toSeq.sortBy(_._2).reverse
      sortedPreferences.head._3.addColumn(new GenericColumn(isValidColumnDefinition, "t"))
      sortedPreferences.tail.map(preference => preference._3.addColumn(new GenericColumn(isValidColumnDefinition, "f")))
      sortedPreferences
    })
    // ( user_id, Seq[(user_id, line_number, GenericRow)] ) => GenericRow
    groupedRows.flatMap(pair => pair._2.map(row => row._3))
  }

  def normalizePreferences(rowsWithValidFlag: RDD[GenericRow], jsonPathBroadcast: Broadcast[Map[String, Seq[UserPreferenceJsonPath]]]): RDD[GenericRow] = {
    import MainQuizUserAnswersCoreEtlVars._
    val jsonSchemaMap = jsonPathBroadcast.value
    rowsWithValidFlag.flatMap(row => {
      val jsonSchemaVersion = row.getColumn("json_schema_version").getValue
      val jsonPaths = jsonSchemaMap(jsonSchemaVersion)
      val preferencesString = row.getColumn("preferences").getValue
      val preferencesJsonString = preferencesString.substring(1, preferencesString.length - 1).replace("\"\"", "\"")
      row.dropColumn("preferences")
      var json: JSONObject = null
      try {
        json = new JSONObject(preferencesJsonString)
      } catch {
        case e: JSONException => logger.error(s"Cannot parse preferences JSON: $row", e)
      }
      var normalizedRows = mutable.Seq[GenericRow]()
      for (jsonPath <- jsonPaths) {
        val quizQuestion = jsonPath.name
        if (jsonPath.isMultiSelect) {
          val answers = json.getJSONArray(quizQuestion)
          for (answerIndex <- 0 until answers.length) {
            val answer = answers.get(answerIndex)
            val newRow = row.clone()
            newRow.addColumn(new GenericColumn(quizQuestionColumnDefinition, quizQuestion))
            newRow.addColumn(new GenericColumn(quizAnswerColumnDefinition, answer.toString))
            normalizedRows :+= newRow
          }
        } else {
          val newRow = row.clone()
          newRow.addColumn(new GenericColumn(quizQuestionColumnDefinition, quizQuestion))
          newRow.addColumn(new GenericColumn(quizAnswerColumnDefinition, json.getString(quizQuestion)))
          normalizedRows :+= newRow
        }
      }
      normalizedRows
    })
  }

  def getAnswerUpdatedUser(rows: RDD[(String, String, Long, GenericRow)]): RDD[String] = {
    rows.filter(row => row._1 == "U").map(row => row._2).distinct()
  }

  val sparkConf = new SparkConf
  val inputFiles = sparkConf.get("spark.ipsy.etl.input.filenames")
  var updateFilename = sparkConf.get("spark.ipsy.etl.output.update")
  var insertFilename = sparkConf.get("spark.ipsy.etl.output.insert")
  val delimiter = sparkConf.get("spark.ipsy.etl.field.delimiter", "|")

  val sc = new SparkContext(sparkConf)

  val pocUtils = new MainQuizUserAnswersCoreEtlPocUtils
  val columnList = pocUtils.getSchemaDefinition.getColumnDefinitions
  val columns = JavaConversions.asScalaBuffer(columnList).seq
  val jsonPaths = pocUtils.getJsonPaths

  val columnsBroadcast = sc.broadcast(columns)
  val jsonPathBroadcast = sc.broadcast(jsonPaths)

  val rows: RDD[(String, Long)] = sc.textFile(inputFiles).zipWithIndex()

  val parsedRows = parseRows(rows, columnsBroadcast, Pattern.quote(delimiter))
  parsedRows.cache()

  val toUpdateAnswerUserIds = getAnswerUpdatedUser(parsedRows)
  toUpdateAnswerUserIds.saveAsTextFile(updateFilename)

  val rowsWithValidFlag = addIsValidColumn(parsedRows.map(row => (row._2, row._3, row._4)))

  val normalizedRows = normalizePreferences(rowsWithValidFlag, jsonPathBroadcast)
  val toInsertRows = normalizedRows.map(_.toJsonString)
  toInsertRows.saveAsTextFile(insertFilename)

  sc.stop()
}
