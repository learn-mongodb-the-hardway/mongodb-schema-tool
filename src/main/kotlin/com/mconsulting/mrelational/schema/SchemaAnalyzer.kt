package com.mconsulting.mrelational.schema

import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonType
import org.bson.BsonValue

//interface DocumentVisitor {
//    fun visit(array: BsonArray)
//    fun visit(document: BsonDocument)
//    fun visit(value: BsonValue)
//}

//interface Element {
//    val name: String?
//}
//
//class FieldType(val type: BsonType) {
//    override fun equals(other: Any?): Boolean {
//        if (!(other is FieldType)) return false
//        return other == type
//    }
//
//    fun merge(fieldType: FieldType) {
//    }
//}
//
//class Field(override val name: String, type: BsonType? = null): Element {
//    private val types = mutableSetOf<FieldType>()
//
//    init {
//        if (type != null) {
//            types.add(FieldType(type))
//        }
//    }
//
//    fun merge(field: Field) {
//        field.types.forEach { fieldType ->
//            if (types.firstOrNull { fieldType.type == it.type } != null) {
//                types.first { it.type == fieldType.type }.merge(fieldType)
//            } else {
//                types.add(fieldType)
//            }
//        }
//    }
//}
//
//class SchemaDocument(override val name: String? = null): Element {
//    private val fields = mutableMapOf<String, Field>()
//
//    fun addField(field: Element) {
//        if (fields.containsKey(field.name)) {
//            fields[field.name]!!.merge(field)
//        } else {
//            fields[field.name] = field
//        }
//    }
//}
//
//class SchemaArray: Element {
//
//}
//
//class SchemaAnalyzerVisitor(schemaDocument: SchemaDocument)  : DocumentVisitor {
//    private val path = mutableListOf(schemaDocument)
//
//    override fun visit(array: BsonArray) {
//        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
//    }
//
//    override fun visit(document: BsonDocument) {
//        document.forEach { key, value ->
//            when (value) {
//                is BsonDocument -> {
//                    val doc = SchemaDocument(key)
//                    path.last().addField(doc)
//                }
//
//                is BsonArray -> {
//
//                }
//
//                else -> {
//                    path.last().addField(Field(key, value.bsonType))
//                }
//            }
//        }
//    }
//
//    override fun visit(value: BsonValue) {
//        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
//    }
//}

class Schema {
    val node: Node = SchemaNode()
}

interface Node

class SchemaArray(val name: String? = null): Node {
    var count = 1
    private val nodes = mutableListOf<Node>()
    val types = mutableSetOf<BsonType>()

    fun addElement(value: BsonValue) {
        when (value) {
            is BsonDocument -> {
                val node = SchemaNode(type = value.bsonType)
                value.forEach {
                    node.addField(it.key, it.value)
                }
                nodes += node
            }

            is BsonArray -> {
                val node = SchemaArray()
                value.forEach {
                    node.addElement(it)
                }
                nodes += node
            }

            else -> {
                nodes += SchemaNode(type = value.bsonType)
            }
        }
    }
}

class SchemaNode(val name: String? = null, val type: BsonType? = null): Node {
    var count = 1
    private val nodes = mutableMapOf<String, Node>()
    val types = mutableSetOf<BsonType>()

    fun addField(key: String, value: BsonValue) {
        mapFields(value, key)
    }

    private fun mapFields(value: BsonValue, key: String) {
        when (value) {
            is BsonDocument -> {
                val node = SchemaNode(key, BsonType.DOCUMENT)
                value.forEach {
                    node.addField(it.key, it.value)
                }

                nodes[key] = node
            }

            is BsonArray -> {
                val node = SchemaArray(key)
                value.forEach {
                    node.addElement(it)
                }

                nodes[key] = node
            }

            else -> {
                if (nodes.containsKey(key)) {
                    (nodes[key]!! as SchemaNode).merge(value)
                } else {
                    nodes[key] = SchemaNode(key, value.bsonType)
                }
            }
        }
    }

    private fun merge(value: BsonValue) {
        types.add(value.bsonType)
    }
}

class SchemaAnalyzer {
    val schema = Schema()
    val path = mutableListOf(schema.node)

    fun process(document: BsonDocument) : Schema {
        document.forEach {
            val node = path.last()
            when (node) {
                is SchemaNode -> {
                    node.addField(it.key, it.value)
                }
                is SchemaArray -> {
                    node.addElement(it.value)
                }
            }
        }

        return schema
    }

    fun process(documents: List<BsonDocument>) : Schema {
        documents.forEach { process(it) }
        return schema
    }
}

//fun BsonDocument.accept(visitor: DocumentVisitor) {
//    visitor.visit(this)
//}
//
//fun BsonArray.accept(visitor: DocumentVisitor) {
//    visitor.visit(this)
//}
//
//fun BsonValue.accept(visitor: DocumentVisitor) {
//    visitor.visit(this)
//}