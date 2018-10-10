package com.mconsulting.mrelational.schema.jsonschema

import com.mconsulting.mrelational.schema.Node
import com.mconsulting.mrelational.schema.Schema
import com.mconsulting.mrelational.schema.SchemaArray
import com.mconsulting.mrelational.schema.SchemaNode
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonString
import org.bson.BsonType

class GeneratorOptions

class Generator(val options: GeneratorOptions = GeneratorOptions()) {
    fun generate(schema: Schema): BsonDocument {
//        val subDocument = BsonDocument()
        val document = BsonDocument()
            .append("\$schema", BsonString("http://json-schema.org/draft-04/schema#"))
            .append("description", BsonString("${schema.db}.${schema.collection} MongoDB Schema"))
            .append("type", BsonString("object"))
//            .append("properties", subDocument)

        process(schema, document, schema.node)

        return document
    }

    private fun process(schema: Schema, document: BsonDocument, node: Node) {
        when (node) {
            is SchemaNode -> processSchemaNode(schema, document, node)
            is SchemaArray -> processSchemaArray(schema, document, node)
        }
    }

    private fun processSchemaArray(schema: Schema, document: BsonDocument, node: SchemaArray) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    private fun processSchemaNode(schema: Schema, document: BsonDocument, node: SchemaNode) {
        // Required fields
        val requiredFields = mutableListOf<BsonString>()

        // Create the properties document
        val properties = BsonDocument()

        // Process all single nodes
        node.nodes.filter { it.value.size == 1 }.map { it.key to it.value.first() }.forEach { pair ->
            when (pair.second) {
                is SchemaNode -> {
                    properties.append(pair.first, BsonDocument()
                            .append("\$type", mapTypeToJsonSchema(pair.second.type)))

                    // Is the times this node was seen the same as the total
                    // number of processed documents in this schema, make the field required
                    if (pair.second.count == schema.node.count) {
                        requiredFields += BsonString(pair.first)
                    }
                }
                else -> throw Exception("Type ${pair.second.javaClass.name} not supported")
            }
        }

        // Add required field if needed
        if (requiredFields.isNotEmpty()) {
            document.append("required", BsonArray(requiredFields))
        }

        // Add the properties
        document.append("properties", properties)

//        node.nodes.forEach { key, list ->
//            if (list.size == 1) {
//                val cnode = list.first()
//
//                when (cnode) {
//                    is SchemaNode -> {
//                        document.append(cnode.name, BsonDocument()
//                            .append("\$type", mapTypeToJsonSchema(cnode.type))
//                        )
//                    }
//                    is SchemaArray -> {
//                        val doc = BsonDocument()
//                        processSchemaArray(schema, doc, cnode)
//                        doc
//                    }
//                    else -> throw Exception("Type ${cnode.javaClass.name} not supported")
//                }
//            } else {
//                list.map {
//                    when (it) {
//                        is SchemaNode -> {
//                            document.append(it.name, BsonDocument()
//                                .append("\$type", mapTypeToJsonSchema(it.type))
//                            )
//                        }
//                        is SchemaArray -> {
//                            val doc = BsonDocument()
//                            processSchemaArray(schema, doc, it)
//                            doc
//                        }
//                        else -> throw Exception("Type ${it.javaClass.name} not supported")
//                    }
//                }
//            }
//        }
    }

    private fun mapTypeToJsonSchema(type: BsonType): BsonString {
        return BsonString(when (type) {
            BsonType.STRING -> "string"
            BsonType.INT32 -> "int"
            BsonType.UNDEFINED -> "undefined"
            BsonType.MIN_KEY -> "minKey"
            BsonType.MAX_KEY -> "maxKey"
            BsonType.REGULAR_EXPRESSION -> "regex"
            BsonType.TIMESTAMP -> "timestamp"
            BsonType.DATE_TIME -> "date"
            BsonType.DECIMAL128 -> "decimal"
            BsonType.OBJECT_ID -> "objectId"
            BsonType.SYMBOL -> "symbol"
            BsonType.DOUBLE -> "double"
            BsonType.BOOLEAN -> "bool"
            BsonType.JAVASCRIPT_WITH_SCOPE -> "javascriptWithScope"
            BsonType.DOCUMENT -> "document"
            BsonType.ARRAY -> "array"
            BsonType.JAVASCRIPT -> "javascript"
            BsonType.INT64 -> "long"
            BsonType.BINARY -> "binary"
            BsonType.DB_POINTER -> "dbPointer"
            BsonType.NULL -> "null"
            else -> throw Exception("type $type not supported")
        })
    }
}

//class SchemaGeneratorVisitor(schema: Schema) : SchemaVisitor {
//    override fun visit(node: SchemaNode) {
//        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
//    }
//
//    override fun visit(array: SchemaArray) {
//        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
//    }
//}