package com.mconsulting.mrelational.schema

import com.mconsulting.mrelational.document
import com.mconsulting.mrelational.registry
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.BsonArray
import org.bson.BsonBinary
import org.bson.BsonBoolean
import org.bson.BsonDateTime
import org.bson.BsonDbPointer
import org.bson.BsonDecimal128
import org.bson.BsonDocument
import org.bson.BsonDouble
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonJavaScript
import org.bson.BsonJavaScriptWithScope
import org.bson.BsonMaxKey
import org.bson.BsonMinKey
import org.bson.BsonNull
import org.bson.BsonObjectId
import org.bson.BsonString
import org.bson.BsonSymbol
import org.bson.BsonType
import org.bson.BsonUndefined
import org.bson.Document
import org.bson.json.JsonWriterSettings
import org.bson.types.Decimal128
import org.bson.types.ObjectId
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.util.*

class SchemaAnalyzerTest {
    @Test
    fun shouldCorrectlyMapAllFieldTypes() {
        val documents = listOf(
            BsonDocument()
                .append("a", BsonInt32(1))
                .append("b", BsonInt64(2))
                .append("c", BsonDouble(1.1))
                .append("d", BsonString("hello"))
                .append("e", BsonBoolean(true))
                .append("f", BsonBinary("hello".toByteArray()))
                .append("g", BsonDocument().append("g1", BsonString("world")))
                .append("h", BsonDateTime(Date().time))
                .append("i", BsonDecimal128(Decimal128.parse("100.10")))
                .append("j", BsonJavaScript("fun() {}"))
                .append("k", BsonJavaScriptWithScope("fun2(){}", BsonDocument().append("k1", BsonInt32(1))))
                .append("o", BsonMaxKey())
                .append("p", BsonMinKey())
                .append("m", BsonNull())
                .append("w", BsonUndefined())
                .append("q", BsonSymbol("q"))
                .append("z", BsonObjectId())
                .append("r", BsonDbPointer("a", ObjectId()))
                .append("s", BsonArray(listOf(BsonString("s1"))))
        )

        // Create an analyser instance and analyze the documents
        val schema = SchemaAnalyzer("db", "coll").process(documents)
        assertEquals(Schema(db = "db", collection = "coll", node = SchemaNode(
            type = BsonType.DOCUMENT,
            count = 2,
            types = mutableSetOf(BsonType.INT32, BsonType.STRING),
            nodes = mutableMapOf(
                "a" to mutableListOf<Node>(SchemaNode(
                    name = "a", count = 1, type = BsonType.INT32
                )),
                "b" to mutableListOf<Node>(SchemaNode(
                    name = "b", count = 1, type = BsonType.INT64
                )),
                "c" to mutableListOf<Node>(SchemaNode(
                    name = "c", count = 1, type = BsonType.DOUBLE
                )),
                "d" to mutableListOf<Node>(SchemaNode(
                    name = "d", count = 1, type = BsonType.STRING
                )),
                "e" to mutableListOf<Node>(SchemaNode(
                    name = "e", count = 1, type = BsonType.BOOLEAN
                )),
                "f" to mutableListOf<Node>(SchemaNode(
                    name = "f", count = 1, type = BsonType.BINARY
                )),
                "g" to mutableListOf<Node>(SchemaNode(
                    name = "g", count = 1, type = BsonType.DOCUMENT, types = mutableSetOf(BsonType.STRING), nodes = mutableMapOf(
                    "g1" to mutableListOf<Node>(SchemaNode(
                        name = "g1", count = 1, type = BsonType.STRING
                    ))
                ))),
                "h" to mutableListOf<Node>(SchemaNode(
                    name = "h", count = 1, type = BsonType.DATE_TIME
                )),
                "i" to mutableListOf<Node>(SchemaNode(
                    name = "i", count = 1, type = BsonType.DECIMAL128
                )),
                "j" to mutableListOf<Node>(SchemaNode(
                    name = "j", count = 1, type = BsonType.JAVASCRIPT
                )),
                "k" to mutableListOf<Node>(JavaScriptWithScopeNode(
                    name = "k", count = 1, type = BsonType.JAVASCRIPT_WITH_SCOPE, scope = SchemaNode(
                        type = BsonType.DOCUMENT, types = mutableSetOf(BsonType.INT32), nodes = mutableMapOf(
                            "k1" to mutableListOf<Node>(SchemaNode(name = "k1", type = BsonType.INT32))
                        )
                    )
                )),
                "o" to mutableListOf<Node>(SchemaNode(
                    name = "o", count = 1, type = BsonType.MAX_KEY
                )),
                "p" to mutableListOf<Node>(SchemaNode(
                    name = "p", count = 1, type = BsonType.MIN_KEY
                )),
                "m" to mutableListOf<Node>(SchemaNode(
                    name = "m", count = 1, type = BsonType.NULL
                )),
                "w" to mutableListOf<Node>(SchemaNode(
                    name = "w", count = 1, type = BsonType.UNDEFINED
                )),
                "q" to mutableListOf<Node>(SchemaNode(
                    name = "q", count = 1, type = BsonType.SYMBOL
                )),
                "z" to mutableListOf<Node>(SchemaNode(
                    name = "z", count = 1, type = BsonType.OBJECT_ID
                )),
                "r" to mutableListOf<Node>(SchemaNode(
                    name = "r", count = 1, type = BsonType.DB_POINTER
                )),
                "s" to mutableListOf<Node>(SchemaArray(
                    name = "s", count = 1, types = mutableSetOf(BsonType.STRING), nodes = mutableListOf(
                    SchemaNode("s1", type = BsonType.STRING)
                )))
            )
        )), schema)
    }

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
        val schema = SchemaAnalyzer("db", "coll").process(documents)
        assertEquals(Schema(db = "db", collection = "coll", node = SchemaNode(
            type = BsonType.DOCUMENT,
            count = 2,
            types = mutableSetOf(BsonType.INT32, BsonType.STRING),
            nodes = mutableMapOf(
                "a" to mutableListOf<Node>(SchemaNode(
                    name = "a", count = 1, type = BsonType.INT32
                ), SchemaNode(
                    name = "a", count = 1, type = BsonType.STRING
                )),
                "b" to mutableListOf<Node>(SchemaNode(
                    name = "b", count = 1, type = BsonType.STRING
                ))
            )
        )), schema)
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

        // Create an analyser instance and analyze the documents
        val schema = SchemaAnalyzer("db", "coll").process(documents)
        assertEquals(Schema(db = "db", collection = "coll", node = SchemaNode(
            type = BsonType.DOCUMENT,
            count = 2,
            types = mutableSetOf(BsonType.INT32, BsonType.DOCUMENT),
            nodes = mutableMapOf(
                "a" to mutableListOf<Node>(SchemaNode(
                    name = "a", count = 1, type = BsonType.INT32
                )),
                "b" to mutableListOf<Node>(SchemaNode(
                    name = "b", count = 1, type = BsonType.DOCUMENT, types = mutableSetOf(BsonType.STRING), nodes = mutableMapOf(
                  "c" to mutableListOf<Node>(SchemaNode(
                      name = "c", count = 1, type = BsonType.STRING
                  ))
                )))
            )
        )), schema)
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

        // Create an analyser instance and analyze the documents
        val schema = SchemaAnalyzer("db", "coll").process(documents)
        assertEquals(Schema(db = "db", collection = "coll", node = SchemaNode(
            type = BsonType.DOCUMENT,
            count = 2,
            types = mutableSetOf(BsonType.INT32, BsonType.ARRAY),
            nodes = mutableMapOf(
                "a" to mutableListOf<Node>(
                    SchemaNode("a", count = 1, type = BsonType.INT32)
                ),
                "b" to mutableListOf<Node>(
                    SchemaArray(
                        name = "b",
                        count = 2,
                        types = mutableSetOf(BsonType.STRING, BsonType.DOCUMENT),
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
                                        count = 1,
                                        type = BsonType.INT32))
                                )
                            )
                        )
                    )
                )
            )
        )), schema)
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

        // Create an analyser instance and analyze the documents
        val schema = SchemaAnalyzer("db", "coll").process(documents)
        assertEquals(Schema(db = "db", collection = "coll", node = SchemaNode(
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

        // Options
        val options = SchemaAnalyzerOptions(true)

        // Create an analyser instance and analyze the documents
        val schema = SchemaAnalyzer("db", "coll", options).process(documents)
        assertEquals(Schema(db = "db", collection = "coll", options = options, node = SchemaNode(
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