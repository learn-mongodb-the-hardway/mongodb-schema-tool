package com.mconsulting.mrelational.schema

import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonType
import org.bson.BsonValue

interface Node {
    val type: BsonType
    var count: Long

    fun merge(node: Node)
    fun inc(size: Long)
}

class Schema(
    val db: String,
    val collection: String,
    private val options: SchemaAnalyzerOptions = SchemaAnalyzerOptions(),
    val node: Node = SchemaNode(options = options, type = BsonType.DOCUMENT, count = 0)) {

    override fun equals(other: Any?): Boolean {
        if (other == null) return false
        if (other !is Schema) return false
        return db == other.db
            && collection == other.collection
            && options == other.options
            && node == other.node
    }
}

class SchemaArray(
    val name: String? = null,
    val options: SchemaAnalyzerOptions = SchemaAnalyzerOptions(),
    val nodes: MutableList<Node> = mutableListOf(),
    val types: MutableSet<BsonType> = mutableSetOf(),
    override var count: Long = 1
): Node {
    override fun inc(size: Long) {
        count += size
        nodes.forEach { it.inc(size) }
    }

    override val type = BsonType.ARRAY

    fun addElement(value: BsonValue) {
        when (value) {
            is BsonDocument -> {
                val node = SchemaNode(type = value.bsonType, options = options)
                value.forEach {
                    node.addField(it.key, it.value)
                }

                // Add type
                types += BsonType.DOCUMENT
                // Get the node
                val tNode = nodes.firstOrNull { it == node }
                // Add the node if it does not exist
                if (tNode == null) {
                    nodes.add(node)
                } else if (options.mergeDocuments) {
                    tNode.merge(node)
                } else {
                    tNode.inc(node.count)
                }
            }

            is BsonArray -> {
                val node = SchemaArray(options = options)
                value.forEach {
                    node.addElement(it)
                }

                // Add type
                types += BsonType.ARRAY
                // Get the node
                val tNode = nodes.firstOrNull { it == node }
                // Add the node if it does not exist
                if (tNode == null) {
                    nodes.add(node)
                } else if (options.mergeDocuments) {
                    tNode.merge(node)
                } else {
                    tNode.inc(node.count)
                }
            }

            else -> {
                val node = SchemaNode(type = value.bsonType, options = options)
                // Add type
                types += node.type
                // Get the node
                val tNode = nodes.firstOrNull { it == node }
                // Add the node if it does not exist
                if (tNode == null) {
                    nodes.add(node)
                } else {
                    tNode.inc(node.count)
                }
            }
        }
    }

    override fun merge(value: Node) {
        // Update the number of times we have seen this element
        count += 1

        if (value !is SchemaArray) {
            throw Exception("cannot merge SchemaArray with SchemaNode")
        }

        // Merge the elements
        value.nodes.forEach { node ->

            // We are going to merge the documents into a single
            // global document type
            if (options.mergeDocuments
                && node is SchemaNode
                && node.type == BsonType.DOCUMENT) {
                val documentNode = nodes
                    .filterIsInstance<SchemaNode>()
                    .firstOrNull { it.type == BsonType.DOCUMENT }
                if (documentNode == null) {
                    nodes += node
                    types += node.type
                } else {
                    documentNode.merge(node)
                }
            } else {
                // Get the node
                val tNode = nodes.firstOrNull { it == node }
                // Add the node if it does not exist
                if (tNode == null) {
                    nodes += node
                    types += node.type
                } else {
                    tNode.inc(node.count)
                }
            }
        }
    }

    override fun equals(other: Any?): Boolean {
        if (other == null) return false
        if (other !is SchemaArray) return false
        if (other.nodes.size != this.nodes.size) return false
        if (other.types.intersect(this.types).size != this.types.size) return false
        if (other.nodes.isEmpty()) return true
        return other.nodes.map { node ->
            this.nodes.firstOrNull { it == node } != null
        }.reduce { acc, b -> acc.and(b) }
    }
}

class SchemaNode(
    val name: String? = null,
    override val type: BsonType,
    val options: SchemaAnalyzerOptions = SchemaAnalyzerOptions(),
    val nodes: MutableMap<String, MutableList<Node>> = mutableMapOf(),
    val types: MutableSet<BsonType> = mutableSetOf(),
    override var count: Long = 1
): Node {
    override fun inc(size: Long) {
        count += size
        nodes.values.forEach { it.forEach { it.inc(size) } }
    }

    fun addField(key: String, value: BsonValue) {
        mapFields(value, key)
    }

    private fun mapFields(value: BsonValue, key: String) {
        // Do we not have an entry, add it
        if (!nodes.containsKey(key)) {
            nodes[key] = mutableListOf()
        }

        when (value) {
            is BsonDocument -> {
                val node = SchemaNode(key, BsonType.DOCUMENT, options)
                value.forEach {
                    node.addField(it.key, it.value)
                }

                val tnode = nodes[key]!!.firstOrNull { it == node }
                // Does this specific node not exist
                if (tnode == null) {
                    nodes[key]!!.add(node)
                    types += node.type
                } else {
                    tnode.inc(node.count)
                }
            }

            is BsonArray -> {
                val node = SchemaArray(name = key, options = options)
                value.forEach {
                    node.addElement(it)
                }

                // Check if we have a BsonArray type already
                // Does this specific node not exist
                if (nodes[key]!!.filterIsInstance<SchemaArray>().isNotEmpty()) {
                    nodes[key]!!.filterIsInstance<SchemaArray>().first().merge(node)
                } else {
                    val tnode = nodes[key]!!.firstOrNull { it == node }
                    // Does this specific node not exist
                    if (tnode == null) {
                        nodes[key]!!.add(node)
                        types += node.type
                    } else {
                        tnode.inc(node.count)
                    }
                }
            }

            else -> {
                val node = SchemaNode(key, value.bsonType, options)

                val tnode = nodes[key]!!.firstOrNull { it == node }
                // Does this specific node not exist
                if (tnode == null) {
                    nodes[key]!!.add(node)
                    types += node.type
                } else {
                    tnode.inc(node.count)
                }
            }
        }
    }

    override fun merge(value: Node) {
        // Update the number of times we have seen this element
        count += 1

        if (value !is SchemaNode) {
            throw Exception("cannot merge SchemaNode with SchemaArray")
        }

        // Merge the elements
        value.nodes.forEach { key, list ->
            if (!this.nodes.containsKey(key)) {
                this.nodes[key] = list.toMutableList()
                this.types += list.map { it.type }
            } else {
                val tlist = this.nodes[key]!!
                list.forEach {  node ->
                    if (!options.mergeDocuments && !tlist.contains(node)) {
                        tlist += node
                        types += node.type
                    } else if (options.mergeDocuments
                        && node is SchemaNode
                        && node.type == BsonType.DOCUMENT) {
                        val doc = tlist
                            .filterIsInstance<SchemaNode>()
                            .firstOrNull()
                        if (doc != null) {
                            doc.merge(node)
                        } else {
                            tlist += node
                            types += node.type
                        }
                    } else if (!tlist.contains(node)) {
                        tlist += node
                        types += node.type
                    } else if (tlist.contains(node)) {
                        tlist.first { it == node }.count += 1
                    }
                }
            }
        }
    }

    override fun equals(other: Any?): Boolean {
        if (other == null) return false
        if (other !is SchemaNode) return false
        if (other.nodes.size != this.nodes.size) return false
        if (other.types.intersect(this.types).size != this.types.size) return false
        if (other.type != this.type) return false
        if (other.nodes.isEmpty()) return true
        return other.nodes.map {
            this.nodes[it.key] == it.value
        }.reduce { acc, b -> acc.and(b) }
    }
}

data class SchemaAnalyzerOptions(val mergeDocuments: Boolean = false)

class SchemaAnalyzer(
    val db: String,
    val collection: String,
    options: SchemaAnalyzerOptions = SchemaAnalyzerOptions()) {
    private val schema = Schema(db, collection, options)
    private val path = mutableListOf(schema.node)

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

        schema.node.count += 1
        return schema
    }

    fun process(documents: List<BsonDocument>) : Schema {
        documents.forEach { process(it) }
        return schema
    }
}