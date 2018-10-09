package com.mconsulting.mrelational.schema

import com.mconsulting.mrelational.document
import com.mconsulting.mrelational.registry
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.BsonDocument
import org.bson.BsonType
import org.bson.Document
import org.bson.json.JsonWriterSettings
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
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

    @Test
    fun shouldMergeArrayTypes() {
        // Create two simple documents
        val documents = listOf(document {
            arrayOf("b") {
                value("hello world")
                documentOf {
                    field("c", 200)
                }
            }
        }, document {
            arrayOf("b") {
                value(100)
                documentOf {
                    field("c", 200)
                }
                documentOf {
                    field("d", "hey")
                }
                documentOf {
                    field("c", 500)
                }
                documentOf {
                    field("c", "reddit")
                }
            }
        }).map { it.toBsonDocument(BsonDocument::class.java, registry) }

//        println(documents.first().toJson(JsonWriterSettings(true)))

        // Create an analyser instance and analyze the documents
        val schema = SchemaAnalyzer().process(documents)
        assertEquals(Schema(node = SchemaNode(
            type = BsonType.DOCUMENT,
            count = 2,
            types = mutableSetOf(BsonType.ARRAY),
            nodes = mutableMapOf(
                "b" to mutableListOf<Node>(
                    SchemaArray(
                        name = "b",
                        count = 2,
                        types = mutableSetOf(BsonType.STRING, BsonType.DOCUMENT, BsonType.INT32),
                        nodes = mutableListOf(
                            SchemaNode(name = null, type = BsonType.STRING, count = 1),
                            SchemaNode(
                                name = null,
                                type = BsonType.DOCUMENT,
                                types = mutableSetOf(BsonType.INT32),
                                count = 3,
                                nodes = mutableMapOf(
                                    "c" to mutableListOf<Node>(SchemaNode(
                                        name = "c",
                                        count = 3,
                                        type = BsonType.INT32))
                                )
                            ),
                            SchemaNode(name = null, type = BsonType.INT32, count = 1),
                            SchemaNode(
                                name = null,
                                type = BsonType.DOCUMENT,
                                types = mutableSetOf(BsonType.STRING),
                                count = 1,
                                nodes = mutableMapOf(
                                    "d" to mutableListOf<Node>(SchemaNode(
                                        name = "d",
                                        count = 1,
                                        type = BsonType.STRING))
                                )
                            ),
                            SchemaNode(
                                name = null,
                                type = BsonType.DOCUMENT,
                                types = mutableSetOf(BsonType.STRING),
                                count = 1,
                                nodes = mutableMapOf(
                                    "c" to mutableListOf<Node>(SchemaNode(
                                        name = "c",
                                        count = 1,
                                        type = BsonType.STRING))
                                )
                            )
                        )
                    )
                )
            )
        )), schema)
    }

    @Test
    fun shouldMergeArrayDocumentTypes() {
        // Create two simple documents
        val documents = listOf(document {
            arrayOf("b") {
                value("hello world")
                documentOf {
                    field("c", 200)
                }
            }
        }, document {
            arrayOf("b") {
                value(100)
                documentOf {
                    field("c", 200)
                }
                documentOf {
                    field("d", "hey")
                }
                documentOf {
                    field("c", 500)
                }
                documentOf {
                    field("c", "reddit")
                }
            }
        }).map { it.toBsonDocument(BsonDocument::class.java, registry) }

//        println(documents.first().toJson(JsonWriterSettings(true)))

        // Options
        val options = SchemaAnalyzerOptions(true)

        // Create an analyser instance and analyze the documents
        val schema = SchemaAnalyzer(options).process(documents)
        assertEquals(Schema(options = options, node = SchemaNode(
            type = BsonType.DOCUMENT,
            count = 2,
            types = mutableSetOf(BsonType.ARRAY),
            nodes = mutableMapOf(
                "b" to mutableListOf<Node>(
                    SchemaArray(
                        name = "b",
                        options = options,
                        count = 2,
                        types = mutableSetOf(BsonType.STRING, BsonType.DOCUMENT, BsonType.INT32),
                        nodes = mutableListOf(
                            SchemaNode(name = null, options = options, type = BsonType.STRING, count = 1),
                            SchemaNode(
                                name = null,
                                options = options,
                                type = BsonType.DOCUMENT,
                                types = mutableSetOf(BsonType.INT32, BsonType.STRING),
                                count = 5,
                                nodes = mutableMapOf(
                                    "c" to mutableListOf<Node>(SchemaNode(
                                        name = "c",
                                        options = options,
                                        count = 3,
                                        type = BsonType.INT32), SchemaNode(
                                        name = "c",
                                        options = options,
                                        count = 1,
                                        type = BsonType.STRING)),
                                    "d" to mutableListOf<Node>(SchemaNode(
                                        name = "d",
                                        options = options,
                                        count = 1,
                                        type = BsonType.STRING))
                                )
                            ),
                            SchemaNode(name = null, options = options, type = BsonType.INT32, count = 1)
                        )
                    )
                )
            )
        )), schema)
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