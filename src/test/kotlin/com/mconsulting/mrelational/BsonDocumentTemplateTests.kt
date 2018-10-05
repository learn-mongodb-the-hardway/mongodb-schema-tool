package com.mconsulting.mrelational

import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonDouble
import org.bson.BsonInt32
import org.bson.BsonString
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.ValueCodecProvider
import org.bson.codecs.configuration.CodecRegistries
import org.bson.json.JsonWriterSettings
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class BsonDocumentTemplateTests {
    @Test
    fun simpleDocumentGenerator() {
        val doc = document {
            field("a", 100)
            field("b", 10.10)
            field("c", "string")

            arrayOf("d") {
                value(100)
                documentOf {
                    field("h", 100)
                }
            }

            documentOf("e") {
                arrayOf("t", 10, 10, 10, 40, 50) {
                    value(200)
                    documentOf {
                        field("h", 100)
                    }
                }
            }
        }

        assertEquals(BsonDocument()
            .append("a", BsonInt32(100))
            .append("b", BsonDouble(10.10))
            .append("c", BsonString("string"))
            .append("d", BsonArray(
                mutableListOf(
                    BsonInt32(100),
                    BsonDocument()
                        .append("h", BsonInt32(100))
                )
            ))
            .append("e", BsonDocument()
                .append("t", BsonArray(
                    mutableListOf(
                        BsonInt32(10),
                        BsonInt32(10),
                        BsonInt32(10),
                        BsonInt32(40),
                        BsonInt32(50),
                        BsonInt32(200),
                        BsonDocument()
                            .append("h", BsonInt32(100))
                    )
                )))
            , doc.toBsonDocument(BsonDocument::class.java, registry))
//        println(doc.toJson(JsonWriterSettings(true)))
    }
}