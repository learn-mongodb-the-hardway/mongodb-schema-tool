package com.mconsulting.mrelational.schema.extractor

import com.mconsulting.mrelational.document
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.io.File

class SchemaExtractorExecutorTest {

    @Test
    fun simpleExtraction() {
        val executor = SchemaExtractorExecutor(SchemaExtractorOptions(
            uri = uri,
            outputFormat = OutputFormat.SCHEMA,
            outputDirectory = File(System.getProperty("java.io.tmpdir")),
            namespaces = listOf(
                Namespace(db.name, extractor1.namespace.collectionName, 10),
                Namespace(db.name, extractor2.namespace.collectionName, 10)
            ),
            mergeDocuments = false
        ))

        val schemas = executor.execute()

        // Print the json schema for each
        schemas.forEach {
//            println(it.toJson())
            println(it.toJson(OutputFormat.SCHEMA))
        }

        println()
    }


    companion object {
        lateinit var client: MongoClient
        lateinit var db: MongoDatabase
        lateinit var extractor1: MongoCollection<Document>
        lateinit var extractor2: MongoCollection<Document>
        private lateinit var uri: MongoClientURI

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            uri = MongoClientURI("mongodb://localhost:27017")
            client = MongoClient(uri)
            db = client.getDatabase("mrelational_tests")
            extractor1 = db.getCollection("extractor_1")
            extractor2 = db.getCollection("extractor_2")

            // Drop collection
            extractor1.drop()
            extractor2.drop()

            // Insert some documents
            extractor1.insertMany(listOf(
                document {
                    field("a", 100)

                    documentOf("b") {
                        field("c", "hello world")
                    }
                }
            ))

            // Insert some documents
            extractor2.insertMany(listOf(
                document {
                    arrayOf("a") {
                        value("hello")

                        documentOf {
                            field("a", 100)
                        }
                    }
                }
            ))
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
            client.close()
        }
    }
}