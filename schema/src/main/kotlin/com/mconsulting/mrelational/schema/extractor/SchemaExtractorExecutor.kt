package com.mconsulting.mrelational.schema.extractor

import com.mconsulting.mrelational.schema.Schema
import com.mconsulting.mrelational.schema.SchemaAnalyzer
import com.mconsulting.mrelational.schema.SchemaAnalyzerOptions
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import org.bson.BsonDocument
import org.bson.BsonElement
import org.bson.Document
import org.bson.conversions.Bson
import java.io.File

data class Namespace(val db: String, val collection: String, val limit: Long)

enum class OutputFormat {
    SCHEMA, JSON_SCHEMA_V4
}

data class SchemaExtractorOptions(
    val uri: MongoClientURI,
    val namespaces: List<Namespace>,
    val outputDirectory: File,
    val outputFormat: OutputFormat
)

class SchemaExtractorExecutor(val options: SchemaExtractorOptions) {
    private var client: MongoClient

    init {
        client = MongoClient(options.uri)
    }

    fun execute() : List<Schema> {
        // For each namespace generate the Schema
        return options.namespaces.map { namespace ->
            // Get the collection we wish to sample
            val db = client.getDatabase(namespace.db)
            val collection = db.getCollection(namespace.collection, BsonDocument::class.java)

            // All aggregation steps
            val aggregationSteps = mutableListOf<Bson>()

            // Build the aggregation pipeline
            if (namespace.limit > 0) {
                aggregationSteps += Document(mapOf(
                    "\$sample" to Document(mapOf(
                        "size" to namespace.limit
                    ))
                ))
            }

            // Create an analyzer
            val analyzer = SchemaAnalyzer(namespace.db, namespace.collection)

            // Sample the collection
            collection
                .aggregate(aggregationSteps)
                .batchSize(100)
                .forEach { document ->
                    analyzer.process(document)
                }

            analyzer.schema
        }
    }
}