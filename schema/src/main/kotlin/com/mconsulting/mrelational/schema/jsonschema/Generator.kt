package com.mconsulting.mrelational.schema.jsonschema

import com.mconsulting.mrelational.schema.Node
import com.mconsulting.mrelational.schema.Schema
import com.mconsulting.mrelational.schema.SchemaArray
import com.mconsulting.mrelational.schema.SchemaNode
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonString
import org.bson.BsonType

class GeneratorOptions(val useJsonTypesWherePossible: Boolean = false)

class Generator(val options: GeneratorOptions = GeneratorOptions()) {
    fun generate(schema: Schema): BsonDocument {
        val document = BsonDocument()
            .append("\$schema", BsonString("http://json-schema.org/draft-04/schema#"))
            .append("description", BsonString("${schema.db}.${schema.collection} MongoDB Schema"))
            .append("type", BsonString("object"))

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
        println()

        if (node.nodes.size > 1) {
            // Generate the schema documents
            val documents = node.nodes.map {
                when (it) {
                    is SchemaNode -> {
                        if (it.type == BsonType.DOCUMENT) {
                            val doc = mapTypeToJsonSchema(it.type, options)
                            processSchemaNode(schema, doc, it)
                            doc
                        } else {
                            mapTypeToJsonSchema(it.type, options)
                        }
                    }
                    is SchemaArray -> {
                        val doc = mapTypeToJsonSchema(it.type, options)
                        processSchemaArray(schema, doc, it)
                        doc
                    }
                    else -> throw Exception("Type ${it.javaClass.name} not supported")
                }
            }

            // Create the anyOf element
            val subDocument = BsonDocument()
                .append("anyOf", BsonArray(documents))

            // Add ot the list of items
            document.append("items", subDocument)
        } else {
            val cnode = node.nodes.first()

            when (cnode) {
                is SchemaNode -> {
                    if (cnode.type == BsonType.DOCUMENT) {
                        val doc = mapTypeToJsonSchema(cnode.type, options)
                        processSchemaNode(schema, doc, cnode)
                        doc
                    } else {
                        document.append("items", mapTypeToJsonSchema(cnode.type, options))
                    }
                }
                is SchemaArray -> {
                    val doc = mapTypeToJsonSchema(cnode.type, options)
                    processSchemaArray(schema, doc, cnode)
                    doc
                }
                else -> throw Exception("Type ${cnode.javaClass.name} not supported")
            }
        }
    }

    private fun processSchemaNode(schema: Schema, document: BsonDocument, node: SchemaNode) {
        // Required fields
        val requiredFields = mutableListOf<BsonString>()

        // Create the properties document
        val properties = BsonDocument()

        // Process all single nodes
        node.nodes.filter { it.value.size == 1 }.map { it.key to it.value.first() }.forEach { pair ->
            val singleNode = pair.second

            when (singleNode) {
                is SchemaNode -> {
                    // If it's not a document or array use the $type extension for MongoDB
                    if (singleNode.type != BsonType.DOCUMENT) {
                        properties.append(pair.first, mapTypeToJsonSchema(singleNode.type, options))
                    } else {
                        val doc = mapTypeToJsonSchema(singleNode.type, options)
                        properties.append(pair.first, doc)
                        processSchemaNode(schema, doc, singleNode)
                    }
                }
                is SchemaArray -> {
                    val doc = mapTypeToJsonSchema(singleNode.type, options)
                    properties.append(pair.first, doc)
                    processSchemaArray(schema, doc, singleNode)
                }
                else -> throw Exception("Type ${pair.second.javaClass.name} not supported")
            }

            // Is the times this node was seen the same as the total
            // number of processed documents in this schema, make the field required
            if (pair.second.count == schema.node.count) {
                requiredFields += BsonString(pair.first)
            }
        }

        // Process all the mixed type nodes
        node.nodes.filter { it.value.size > 1 }.forEach { key, list ->
            // Add up the counts of each shape seen
            val count = list.map { it.count }.sum()

            val documents = list.map { singleNode ->
                var doc = BsonDocument()

                if (singleNode.type == BsonType.DOCUMENT && singleNode is SchemaNode) {
                    processSchemaNode(schema, doc, singleNode)
                } else if (singleNode.type == BsonType.ARRAY && singleNode is SchemaArray) {
                    processSchemaArray(schema, doc, singleNode)
                } else {
                    doc = mapTypeToJsonSchema(singleNode.type, options)
                }

                doc
            }

            properties.append(key, BsonDocument()
                .append("oneOf", BsonArray(documents)))

            // Is this a required field
            if (count == schema.node.count) {
                requiredFields += BsonString(key)
            }
        }

        // Add required field if needed
        if (requiredFields.isNotEmpty()) {
            document.append("required", BsonArray(requiredFields))
        }

        // Add the properties
        document.append("properties", properties)
    }

    private fun mapTypeToJsonSchema(type: BsonType, options: GeneratorOptions): BsonDocument {
        if (options.useJsonTypesWherePossible) {
            return when (type) {
                BsonType.STRING -> BsonDocument().append("type", BsonString("string"))
                BsonType.INT32 -> BsonDocument().append("type", BsonString("integer"))
                BsonType.DOUBLE -> BsonDocument().append("type", BsonString("number"))
                BsonType.NULL -> BsonDocument().append("type", BsonString("null"))
                BsonType.BOOLEAN -> BsonDocument().append("type", BsonString("boolean"))
                BsonType.DOCUMENT -> BsonDocument().append("type", BsonString("object"))
                BsonType.ARRAY -> BsonDocument().append("type", BsonString("array"))
                BsonType.UNDEFINED -> BsonDocument().append("\$type", BsonString("undefined"))
                BsonType.MIN_KEY -> BsonDocument().append("\$type", BsonString("minKey"))
                BsonType.MAX_KEY -> BsonDocument().append("\$type", BsonString("maxKey"))
                BsonType.REGULAR_EXPRESSION -> BsonDocument().append("\$type", BsonString("regex"))
                BsonType.TIMESTAMP -> BsonDocument().append("\$type", BsonString("timestamp"))
                BsonType.DATE_TIME -> BsonDocument().append("\$type", BsonString("date"))
                BsonType.DECIMAL128 -> BsonDocument().append("\$type", BsonString("decimal"))
                BsonType.OBJECT_ID -> BsonDocument().append("\$type", BsonString("objectId"))
                BsonType.SYMBOL -> BsonDocument().append("\$type", BsonString("symbol"))
                BsonType.JAVASCRIPT_WITH_SCOPE -> BsonDocument().append("\$type", BsonString("javascriptWithScope"))
                BsonType.JAVASCRIPT -> BsonDocument().append("\$type", BsonString("javascript"))
                BsonType.INT64 -> BsonDocument().append("\$type", BsonString("long"))
                BsonType.BINARY -> BsonDocument().append("\$type", BsonString("binary"))
                BsonType.DB_POINTER -> BsonDocument().append("\$type", BsonString("dbPointer"))
                else -> throw Exception("type $type not supported")
            }
        } else {
            return when (type) {
                BsonType.DOCUMENT -> BsonDocument().append("type", BsonString("object"))
                BsonType.ARRAY -> BsonDocument().append("type", BsonString("array"))
                BsonType.STRING -> BsonDocument().append("\$type", BsonString("string"))
                BsonType.INT32 -> BsonDocument().append("\$type", BsonString("int"))
                BsonType.UNDEFINED -> BsonDocument().append("\$type", BsonString("undefined"))
                BsonType.MIN_KEY -> BsonDocument().append("\$type", BsonString("minKey"))
                BsonType.MAX_KEY -> BsonDocument().append("\$type", BsonString("maxKey"))
                BsonType.REGULAR_EXPRESSION -> BsonDocument().append("\$type", BsonString("regex"))
                BsonType.TIMESTAMP -> BsonDocument().append("\$type", BsonString("timestamp"))
                BsonType.DATE_TIME -> BsonDocument().append("\$type", BsonString("date"))
                BsonType.DECIMAL128 -> BsonDocument().append("\$type", BsonString("decimal"))
                BsonType.OBJECT_ID -> BsonDocument().append("\$type", BsonString("objectId"))
                BsonType.SYMBOL -> BsonDocument().append("\$type", BsonString("symbol"))
                BsonType.DOUBLE -> BsonDocument().append("\$type", BsonString("double"))
                BsonType.BOOLEAN -> BsonDocument().append("\$type", BsonString("bool"))
                BsonType.JAVASCRIPT_WITH_SCOPE -> BsonDocument().append("\$type", BsonString("javascriptWithScope"))
                BsonType.JAVASCRIPT -> BsonDocument().append("\$type", BsonString("javascript"))
                BsonType.INT64 -> BsonDocument().append("\$type", BsonString("long"))
                BsonType.BINARY -> BsonDocument().append("\$type", BsonString("binary"))
                BsonType.DB_POINTER -> BsonDocument().append("\$type", BsonString("dbPointer"))
                BsonType.NULL -> BsonDocument().append("\$type", BsonString("null"))
                else -> throw Exception("type $type not supported")
            }
        }
    }
}