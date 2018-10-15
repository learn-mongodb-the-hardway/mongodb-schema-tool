package com.mconsulting.mrelational.schema.json

import com.mconsulting.mrelational.schema.Node
import com.mconsulting.mrelational.schema.Schema
import com.mconsulting.mrelational.schema.SchemaArray
import com.mconsulting.mrelational.schema.SchemaNode
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonString
import org.bson.BsonType

class MongoDraft4GeneratorOptions()

class MongoDraft4Generator(val options: MongoDraft4GeneratorOptions = MongoDraft4GeneratorOptions()) : JsonGenerator {
    override fun generate(schema: Schema): BsonDocument {
        var document = BsonDocument()
            .append("description", BsonString("${schema.db}.${schema.collection} MongoDB Schema"))
            .append("bsonType", BsonString("object"))

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
                        var doc = mapTypeToJsonSchema(singleNode.type, options)
                        properties.append(pair.first, doc)
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

    private fun mapTypeToJsonSchema(type: BsonType, options: MongoDraft4GeneratorOptions): BsonDocument {
        return when (type) {
            BsonType.STRING -> BsonDocument().append("bsonType", BsonString("string"))
            BsonType.INT32 -> BsonDocument().append("bsonType", BsonString("int"))
            BsonType.DOUBLE -> BsonDocument().append("bsonType", BsonString("double"))
            BsonType.NULL -> BsonDocument().append("bsonType", BsonString("null"))
            BsonType.BOOLEAN -> BsonDocument().append("bsonType", BsonString("bool"))
            BsonType.DOCUMENT -> BsonDocument().append("bsonType", BsonString("object"))
            BsonType.ARRAY -> BsonDocument().append("bsonType", BsonString("array"))
            BsonType.UNDEFINED -> BsonDocument().append("bsonType", BsonString("undefined"))
            BsonType.MIN_KEY -> BsonDocument().append("bsonType", BsonString("minKey"))
            BsonType.MAX_KEY -> BsonDocument().append("bsonType", BsonString("maxKey"))
            BsonType.REGULAR_EXPRESSION -> BsonDocument().append("bsonType", BsonString("regex"))
            BsonType.TIMESTAMP -> BsonDocument().append("bsonType", BsonString("timestamp"))
            BsonType.DATE_TIME -> BsonDocument().append("bsonType", BsonString("date"))
            BsonType.DECIMAL128 -> BsonDocument().append("bsonType", BsonString("decimal"))
            BsonType.OBJECT_ID -> BsonDocument().append("bsonType", BsonString("objectId"))
            BsonType.SYMBOL -> BsonDocument().append("bsonType", BsonString("symbol"))
            BsonType.JAVASCRIPT_WITH_SCOPE -> BsonDocument().append("bsonType", BsonString("javascriptWithScope"))
            BsonType.JAVASCRIPT -> BsonDocument().append("bsonType", BsonString("javascript"))
            BsonType.INT64 -> BsonDocument().append("bsonType", BsonString("long"))
            BsonType.BINARY -> BsonDocument().append("bsonType", BsonString("binary"))
            BsonType.DB_POINTER -> BsonDocument().append("bsonType", BsonString("dbPointer"))
            else -> throw Exception("type $type not supported")
        }
    }
}