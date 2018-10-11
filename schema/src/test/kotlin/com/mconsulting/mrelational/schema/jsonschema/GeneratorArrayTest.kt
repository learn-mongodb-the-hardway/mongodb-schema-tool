package com.mconsulting.mrelational.schema.jsonschema

import com.mconsulting.mrelational.document
import com.mconsulting.mrelational.registry
import com.mconsulting.mrelational.schema.SchemaAnalyzer
import com.mconsulting.mrelational.schema.SchemaAnalyzerOptions
import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.BsonDocument
import org.bson.Document
import org.bson.json.JsonWriterSettings
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

class GeneratorArrayTest {
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
        val jsonSchema = Generator(GeneratorOptions(true)).generate(schema)
        // Expected
        val expectedSchema = BsonDocument.parse("""
            {
              "${'$'}schema" : "http://json-schema.org/draft-04/schema#",
              "description" : "db.coll MongoDB Schema",
              "type" : "object",
              "required" : ["a"],
              "properties" : {
                "a" : {
                  "type" : "array",
                  "items" : {
                    "anyOf" : [{
                        "type" : "integer"
                      }, {
                        "type" : "object",
                        "required" : ["b"],
                        "properties" : {
                          "b" : {
                            "type" : "number"
                          }
                        }
                      }, {
                        "type" : "array",
                        "items" : {
                          "type" : "string"
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
        val jsonSchema = Generator(GeneratorOptions(true)).generate(schema)
        // Expected
        val expectedSchema = BsonDocument.parse("""
            {
              "${'$'}schema" : "http://json-schema.org/draft-04/schema#",
              "description" : "db.coll MongoDB Schema",
              "type" : "object",
              "required" : ["a"],
              "properties" : {
                "a" : {
                  "type" : "array",
                  "items" : {
                    "type" : "integer"
                  }
                }
              }
            }
        """.trimIndent())
//        println(jsonSchema.toJson(JsonWriterSettings(true)))
//        println(expectedSchema.toJson(JsonWriterSettings(true)))
        assertEquals(expectedSchema, jsonSchema)
    }

    companion object {
        lateinit var client: MongoClient
        lateinit var db: MongoDatabase
        lateinit var tests: MongoCollection<Document>

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
//            client = MongoClient(MongoClientURI("mongodb://localhost:27017"))
//            db = client.getDatabase("mrelational_tests")
//            tests = db.getCollection("media")
//
//            // Drop collection
//            tests.drop()
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
//            client.close()
        }
    }
}