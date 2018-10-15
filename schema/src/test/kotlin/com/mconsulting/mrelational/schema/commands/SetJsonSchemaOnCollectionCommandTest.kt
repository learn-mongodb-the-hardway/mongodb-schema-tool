package com.mconsulting.mrelational.schema.commands

import com.mconsulting.mrelational.document
import com.mconsulting.mrelational.registry
import com.mconsulting.mrelational.schema.SchemaAnalyzer
import com.mconsulting.mrelational.schema.extractor.OutputFormat
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.MongoWriteException
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.BsonDocument
import org.bson.Document
import org.bson.types.ObjectId
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals

class SetJsonSchemaOnCollectionCommandTest {

    @Test
    fun simpleJsonIntegrationTest() {
        // Create a json schema from a dummy document
        val documents = listOf(
            document {
                field("a", 100)
                field("b", "hello")
                field("c", 10.1)
            }.toBsonDocument(BsonDocument::class.java, registry)
        )

        // Generate the schema
        val analyzer = SchemaAnalyzer(db.name, "integration_1")
        val schema = analyzer.process(documents = documents)
        // Transform the schema into BsonDocument
        val bsonSchema = schema.toBson(OutputFormat.MONGODB_SCHEMA_V4)
        // Apply the schema to the collection
        val command = SetJsonSchemaOnCollectionCommand(client)
            .execute(db.name, integration_1.namespace.collectionName, bsonSchema, ValidationOptions(ValidationLevel.STRICT))

        // Successfully insert a document
        integration_1.insertOne(document {
            field("a", 100)
            field("b", "hello")
            field("c", 10.1)
        }.toBsonDocument(BsonDocument::class.java, registry))

        // Fail to insert a document
        val exception = assertThrows<MongoWriteException> {
            integration_1.insertOne(document {
                field("a", "string")
            }.toBsonDocument(BsonDocument::class.java, registry))
        }

        assertEquals(121, exception.code)
        assertEquals("Document failed validation", exception.message)
    }

    companion object {
        lateinit var client: MongoClient
        lateinit var db: MongoDatabase
        lateinit var integration_1: MongoCollection<BsonDocument>
        private lateinit var uri: MongoClientURI

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            uri = MongoClientURI("mongodb://localhost:27017")
            client = MongoClient(uri)
            db = client.getDatabase("mrelational_tests")
            integration_1 = db.getCollection("integration_1", BsonDocument::class.java)

            // Drop collection
            integration_1.drop()

            // Create the collection
            db.createCollection(integration_1.namespace.collectionName)

//            // Insert some documents
//            extractor1.insertMany(listOf(
//                document {
//                    field("a", 100)
//
//                    documentOf("b") {
//                        field("c", "hello world")
//                    }
//                }
//            ))
//
//            // Insert some documents
//            extractor2.insertMany(listOf(
//                document {
//                    arrayOf("a") {
//                        value("hello")
//
//                        documentOf {
//                            field("a", 100)
//                        }
//                    }
//                }
//            ))
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
            client.close()
        }
    }
}