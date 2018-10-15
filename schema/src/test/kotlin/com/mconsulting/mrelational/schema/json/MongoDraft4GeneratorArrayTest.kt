package com.mconsulting.mrelational.schema.json

import com.mconsulting.mrelational.document
import com.mconsulting.mrelational.registry
import com.mconsulting.mrelational.schema.SchemaAnalyzer
import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.BsonDocument
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

class MongoDraft4GeneratorArrayTest {
    @Test
    fun generateMixedArrayJsonSchema() {
        // Create a document, analyze it and then generate
        // the json schema from it.
        val documents = listOf(
            document {
                arrayOf("a") {
                    value(100)
                    documentOf {
                        field("b", 10.10)
                    }
                    arrayOf {
                        value("hello")
                    }
                }
            }.toBsonDocument(BsonDocument::class.java, registry)
        )

        // Create the analyzer
        val analyzer = SchemaAnalyzer("db", "coll")
        // Get the schema
        val schema = analyzer.process(documents)
        // Generate the json schema
        val jsonSchema = MongoDraft4Generator(MongoDraft4GeneratorOptions()).generate(schema)
        // Expected
        val expectedSchema = BsonDocument.parse("""
            {
              "description" : "db.coll MongoDB Schema",
              "bsonType" : "object",
              "required" : ["a"],
              "properties" : {
                "a" : {
                  "bsonType" : "array",
                  "items" : {
                    "anyOf" : [{
                        "bsonType" : "int"
                      }, {
                        "bsonType" : "object",
                        "required" : ["b"],
                        "properties" : {
                          "b" : {
                            "bsonType" : "double"
                          }
                        }
                      }, {
                        "bsonType" : "array",
                        "items" : {
                          "bsonType" : "string"
                        }
                      }]
                  }
                }
              }
            }
        """.trimIndent())
//        println(jsonSchema.toJson(JsonWriterSettings(true)))
//        println(expectedSchema.toJson(JsonWriterSettings(true)))
        assertEquals(expectedSchema, jsonSchema)
    }

    @Test
    fun generateSingleTypeArrayJsonSchema() {
        // Create a document, analyze it and then generate
        // the json schema from it.
        val documents = listOf(
            document {
                arrayOf("a") {
                    value(100)
                }
            }.toBsonDocument(BsonDocument::class.java, registry)
        )

        // Create the analyzer
        val analyzer = SchemaAnalyzer("db", "coll")
        // Get the schema
        val schema = analyzer.process(documents)
        // Generate the json schema
        val jsonSchema = MongoDraft4Generator(MongoDraft4GeneratorOptions()).generate(schema)
        // Expected
        val expectedSchema = BsonDocument.parse("""
            {
              "description" : "db.coll MongoDB Schema",
              "bsonType" : "object",
              "required" : ["a"],
              "properties" : {
                "a" : {
                  "bsonType" : "array",
                  "items" : {
                    "bsonType" : "int"
                  }
                }
              }
            }
        """.trimIndent())
//        println(jsonSchema.toJson(JsonWriterSettings(true)))
//        println(expectedSchema.toJson(JsonWriterSettings(true)))
        assertEquals(expectedSchema, jsonSchema)
    }
}