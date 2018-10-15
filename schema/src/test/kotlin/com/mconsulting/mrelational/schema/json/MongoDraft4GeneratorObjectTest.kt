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

class MongoDraft4GeneratorObjectTest {

    @Test
    fun generateSimpleJsonSchema() {
        // Create a document, analyze it and then generate
        // the json schema from it.
        val documents = listOf(
            document {
                field("a", 100)
                field("b", "string")
            }.toBsonDocument(BsonDocument::class.java, registry)
        )

        // Create the analyzer
        val analyzer = SchemaAnalyzer("db", "coll")
        // Get the schema
        val schema = analyzer.process(documents)
        // Generate the json schema
        val jsonSchema = MongoDraft4Generator().generate(schema)
        // Expected
        val expectedSchema = BsonDocument.parse("""
            {
              "description" : "db.coll MongoDB Schema",
              "bsonType" : "object",
              "required" : ["a", "b"],
              "properties" : {
                "a" : {
                  "bsonType" : "int"
                },
                "b" : {
                  "bsonType" : "string"
                }
              }
            }
        """.trimIndent())
//        println(jsonSchema.toJson(JsonWriterSettings(true)))
//        println(expectedSchema.toJson(JsonWriterSettings(true)))
        assertEquals(expectedSchema, jsonSchema)
    }

    @Test
    fun generateSimpleNestedJsonSchema() {
        // Create a document, analyze it and then generate
        // the json schema from it.
        val documents = listOf(
            document {
                field("a", 100)
                field("b", "string")

                documentOf("c") {
                    field("d", 10.10)
                    field("e", "hello")
                }
            }.toBsonDocument(BsonDocument::class.java, registry)
        )

        // Create the analyzer
        val analyzer = SchemaAnalyzer("db", "coll")
        // Get the schema
        val schema = analyzer.process(documents)
        // Generate the json schema
        val jsonSchema = MongoDraft4Generator().generate(schema)
        // Expected
        val expectedSchema = BsonDocument.parse("""
            {
              "description" : "db.coll MongoDB Schema",
              "bsonType" : "object",
              "required" : ["a", "b", "c"],
              "properties" : {
                "a" : {
                  "bsonType" : "int"
                },
                "b" : {
                  "bsonType" : "string"
                },
                "c" : {
                  "bsonType" : "object",
                  "required" : ["d", "e"],
                  "properties" : {
                    "d" : {
                      "bsonType" : "double"
                    },
                    "e" : {
                      "bsonType" : "string"
                    }
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
    fun mixedGenerateSimpleJsonSchema() {
        // Create a document, analyze it and then generate
        // the json schema from it.
        val documents = listOf(
            document {
                field("a", 100)
                field("b", "string")
            }.toBsonDocument(BsonDocument::class.java, registry),
            document {
                field("a", "world")
                field("b", "string")
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
              "required" : ["b", "a"],
              "properties" : {
                "b" : {
                  "bsonType" : "string"
                },
                "a" : {
                  "oneOf" : [{
                      "bsonType" : "int"
                    }, {
                      "bsonType" : "string"
                    }]
                }
              }
            }
        """.trimIndent())
//        println(jsonSchema.toJson(JsonWriterSettings(true)))
//        println(expectedSchema.toJson(JsonWriterSettings(true)))
        assertEquals(expectedSchema, jsonSchema)
    }

    @Test
    fun mixedGenerateSimpleNestedJsonSchema() {
        // Create a document, analyze it and then generate
        // the json schema from it.
        val documents = listOf(
            document {
                field("a", 100)
                field("b", "string")

                documentOf("c") {
                    field("d", 10.10)
                    field("e", "hello")
                }
            }.toBsonDocument(BsonDocument::class.java, registry),
            document {
                field("a", 100)
                field("b", "string")

                documentOf("c") {
                    field("d", "world")
                    field("e", "hello")
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
              "required" : ["a", "b", "c"],
              "properties" : {
                "a" : {
                  "bsonType" : "int"
                },
                "b" : {
                  "bsonType" : "string"
                },
                "c" : {
                  "oneOf" : [{
                      "properties" : {
                        "d" : {
                          "bsonType" : "double"
                        },
                        "e" : {
                          "bsonType" : "string"
                        }
                      }
                    }, {
                      "properties" : {
                        "d" : {
                          "bsonType" : "string"
                        },
                        "e" : {
                          "bsonType" : "string"
                        }
                      }
                    }]
                }
              }
            }
        """.trimIndent())
//        println(jsonSchema.toJson(JsonWriterSettings(true)))
//        println(expectedSchema.toJson(JsonWriterSettings(true)))
        assertEquals(expectedSchema, jsonSchema)
    }
}