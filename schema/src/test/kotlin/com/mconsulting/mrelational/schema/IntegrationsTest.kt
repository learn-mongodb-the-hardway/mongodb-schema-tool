package com.mconsulting.mrelational.schema

import com.mconsulting.mrelational.readResourceAsString
import com.mconsulting.mrelational.schema.json.MongoDraft4Generator
import org.bson.BsonDocument
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class IntegrationsTest {
    @Test
    fun extractComplexMediaJson() {
        // Read the json file
        val json = readResourceAsString("json/media.json")
        val expectedJson = readResourceAsString("expected/media.json")
        // Create a BsonDocument
        val document = BsonDocument.parse(json)
        // Create an analyser instance and analyze the documents
        val schema = SchemaAnalyzer("db", "coll", SchemaAnalyzerOptions(false)).process(document)
        // Turn into draft
        val jsonSchema = MongoDraft4Generator().generate(schema)
        // Assertions
        assertEquals(BsonDocument.parse(expectedJson), jsonSchema)
    }
}