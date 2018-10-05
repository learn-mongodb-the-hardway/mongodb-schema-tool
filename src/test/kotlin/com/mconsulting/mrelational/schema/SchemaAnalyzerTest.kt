package com.mconsulting.mrelational.schema

import com.mconsulting.mrelational.document
import com.mconsulting.mrelational.registry
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.BsonDocument
import org.bson.Document
import org.bson.json.JsonWriterSettings
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

class SchemaAnalyzerTest {

    @Test
    fun shouldReturnSimpleTopLevelAnalysedDocument() {
        // Create two simple documents
        val documents = listOf(document {
            field("a", 100)
        }, document {
            field("a", "hello world")
            field("b", "hello world")
        }).map { it.toBsonDocument(BsonDocument::class.java, registry) }

        // Create an analyser instance and analyze the documents
        val schema = SchemaAnalyzer().process(documents)
        println()
    }

    @Test
    fun shouldReturnSimpleNestedDocument() {
        // Create two simple documents
        val documents = listOf(document {
            field("a", 100)
            documentOf("b") {
                field("c", "hello world")
            }
        }).map { it.toBsonDocument(BsonDocument::class.java, registry) }

        println(documents.first().toJson(JsonWriterSettings(true)))

        // Create an analyser instance and analyze the documents
        val schema = SchemaAnalyzer().process(documents)
        println()
    }

    @Test
    fun shouldReturnArraySchema() {
        // Create two simple documents
        val documents = listOf(document {
            field("a", 100)
            arrayOf("b") {
                value("hello world")
                documentOf {
                    field("c", 200)
                }
            }
        }).map { it.toBsonDocument(BsonDocument::class.java, registry) }

        println(documents.first().toJson(JsonWriterSettings(true)))

        // Create an analyser instance and analyze the documents
        val schema = SchemaAnalyzer().process(documents)
        println()
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