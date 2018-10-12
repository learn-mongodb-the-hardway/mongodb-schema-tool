package com.mconsulting.mrelational.schema.json

import com.mconsulting.mrelational.schema.Node
import com.mconsulting.mrelational.schema.Schema
import com.mconsulting.mrelational.schema.SchemaArray
import com.mconsulting.mrelational.schema.SchemaNode
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonElement
import org.bson.BsonInt64
import org.bson.BsonString

class SchemaGeneratorOptions

class SchemaGenerator(val options: SchemaGeneratorOptions = SchemaGeneratorOptions()) : JsonGenerator {
    override fun generate(schema: Schema): BsonDocument {
        val doc = BsonDocument()
        val document = BsonDocument()
            .append("description", BsonString("${schema.db}.${schema.collection} MongoDB Schema"))
            .append("root", doc)

        processNode(schema, doc, schema.node)

        return document
    }

    private fun processNode(schema: Schema, document: BsonDocument, node: Node) : BsonDocument {
        document.append("types", BsonArray(node.types.map {
            BsonString(it.name)
        })).append("type", BsonString(node.type.name))

        when (node) {
            is SchemaNode -> {
                document
                    .append("count", BsonInt64(node.count))
                    .append("nodes", BsonDocument(node.nodes.map {
                        BsonElement(it.key, BsonArray(it.value.map {
                            processNode(schema, BsonDocument(), it)
                        }))
                    }))
            }
            is SchemaArray -> {
                document
                    .append("count", BsonInt64(node.count))
                    .append("nodes", BsonArray(node.nodes.map {
                        processNode(schema, BsonDocument(), it)
                    }))
            }
            else -> throw Exception("Type ${node.javaClass.name} not supported")
        }

        return document
    }
}