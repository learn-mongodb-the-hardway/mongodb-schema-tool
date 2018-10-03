package com.mconsulting.mrelational

import org.bson.json.JsonWriterSettings
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

//        println()
        println(doc.toJson(JsonWriterSettings(true)))
    }
}