package com.ipsy.spark.etl.core

import com.ipsy.data.schema.{ColumnDefinition, SchemaDefinition}

class MainQuizUserAnswersCoreEtlPocUtils {
  def getSchemaDefinition: SchemaDefinition = {
    val schemaDefinition = new SchemaDefinition
    schemaDefinition.setDbIdentifier("db-staging.ipsy.com")
    schemaDefinition.setTableName("public.main_quiz_user_answers")
    val columnDefinitions = schemaDefinition.getColumnDefinitions
    columnDefinitions.add(new ColumnDefinition("id", null, "character varying", null, false, 1, "main_quiz_user_answers"))
    columnDefinitions.add(new ColumnDefinition("version", null, "bigint", "0", true, 2, "main_quiz_user_answers"))
    columnDefinitions.add(new ColumnDefinition("user_id", null, "character varying", null, false, 3, "main_quiz_user_answers"))
    columnDefinitions.add(new ColumnDefinition("date_created", null, "timestamp", null, false, 4, "main_quiz_user_answers"))
    columnDefinitions.add(new ColumnDefinition("date_deleted", null, "timestamp", null, true, 5, "main_quiz_user_answers"))
    columnDefinitions.add(new ColumnDefinition("last_updated", null, "timestamp", null, false, 6, "main_quiz_user_answers"))
    columnDefinitions.add(new ColumnDefinition("preferences", null, "jsonb", null, false, 7, "main_quiz_user_answers"))
    columnDefinitions.add(new ColumnDefinition("json_schema_version", null, "character varying", null, false, 8, "main_quiz_user_answers"))
    schemaDefinition
  }

  def getJsonPaths: Map[String, Seq[UserPreferenceJsonPath]] = {
    val jsonPathV1 = Seq(
      UserPreferenceJsonPath("0ef82f28-96b1-484a-ad1b-9c68f329f447", "$['0ef82f28-96b1-484a-ad1b-9c68f329f447']", true),
      UserPreferenceJsonPath("1739b568-8e53-4b8d-b97b-c9d147c0aea9", "$['1739b568-8e53-4b8d-b97b-c9d147c0aea9']", true),
      UserPreferenceJsonPath("1fe98a31-6682-4932-a02e-45e72defa347", "$['1fe98a31-6682-4932-a02e-45e72defa347']", false),
      UserPreferenceJsonPath("3d8c073b-b8e5-4212-a264-4c66b98b3824", "$['3d8c073b-b8e5-4212-a264-4c66b98b3824']", true),
      UserPreferenceJsonPath("56b9d9fd-cb36-4d7c-93b2-ad3f071ada26", "$['56b9d9fd-cb36-4d7c-93b2-ad3f071ada26']", true),
      UserPreferenceJsonPath("6958171b-099d-4228-bf82-8a9d363953ae", "$['6958171b-099d-4228-bf82-8a9d363953ae']", true),
      UserPreferenceJsonPath("78bcce48-6de7-45bc-a9e2-d78bf573a6ee", "$['78bcce48-6de7-45bc-a9e2-d78bf573a6ee']", false),
      UserPreferenceJsonPath("7a3a83d2-5350-4603-8fe6-867d44543e07", "$['7a3a83d2-5350-4603-8fe6-867d44543e07']", false),
      UserPreferenceJsonPath("87d36085-5125-44e6-8da3-17938d17d5e8", "$['87d36085-5125-44e6-8da3-17938d17d5e8']", true),
      UserPreferenceJsonPath("8b179ebd-40a1-4f75-88c9-8e0db7ec763f", "$['8b179ebd-40a1-4f75-88c9-8e0db7ec763f']", true),
      UserPreferenceJsonPath("93a46150-64f0-41b3-809c-332ee8614953", "$['93a46150-64f0-41b3-809c-332ee8614953']", true),
      UserPreferenceJsonPath("9825e985-2d16-444b-a632-f2bc2fcfad57", "$['9825e985-2d16-444b-a632-f2bc2fcfad57']", true),
      UserPreferenceJsonPath("99a03f99-71a4-43ad-ba07-c08a287aa597", "$['99a03f99-71a4-43ad-ba07-c08a287aa597']", false),
      UserPreferenceJsonPath("bdca3435-49d2-4e22-8432-c1fc24bcab99", "$['bdca3435-49d2-4e22-8432-c1fc24bcab99']", true),
      UserPreferenceJsonPath("c27aefaf-5cd6-439a-96df-ee5c3adad5c5", "$['c27aefaf-5cd6-439a-96df-ee5c3adad5c5']", true),
      UserPreferenceJsonPath("ced9abf9-1360-4f2a-a257-feb48f42bd94", "$['ced9abf9-1360-4f2a-a257-feb48f42bd94']", true),
      UserPreferenceJsonPath("db33867b-877c-4b3c-8b7a-e54337ceb33a", "$['db33867b-877c-4b3c-8b7a-e54337ceb33a']", true),
      UserPreferenceJsonPath("ef4232ac-0794-467d-8c59-0f130f990933", "$['ef4232ac-0794-467d-8c59-0f130f990933']", true),
      UserPreferenceJsonPath("fef59366-ec19-4414-b6d6-c02dd5f02d60", "$['fef59366-ec19-4414-b6d6-c02dd5f02d60']", false)
    )
    Map("1.0" -> jsonPathV1)
  }
}
