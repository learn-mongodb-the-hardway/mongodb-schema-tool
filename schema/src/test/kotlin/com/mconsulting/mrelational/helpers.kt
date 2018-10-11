package com.mconsulting.mrelational

import org.bson.Document
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.ValueCodecProvider
import org.bson.codecs.configuration.CodecRegistries
import java.util.*

@DslMarker
annotation class HelperMarker

@HelperMarker
sealed class Helper<T> {
    abstract fun build(): T
}

interface Field {
    val name: String?
}

class DocumentTemplate(override val name: String? = null, val fields: MutableList<Field> = mutableListOf()) : Field

class DocumentArray(override val name: String? = null, val fields: MutableList<Field> = mutableListOf()): Field

class Value(val value: Any?, override val name: String? = null) : Field

fun document(init: DocumentHelper.() -> Unit): Document {
    val b = DocumentHelper()
    b.init()
    return Document(mapDocument(b.build().fields))
}

sealed class FieldCollectionHelper<T> : Helper<T>() {
    val fields = mutableListOf<Field>()

    fun field(name: String, value: Any?) {
        when (value) {
            null,
            is String,
            is Int,
            is Double,
            is Long,
            is Short,
            is Byte,
            is Float,
            is Date -> {
                fields += PrimitiveField(name, value)
            }
            else -> throw Exception("type ${value.javaClass.name}")
        }
    }

    fun arrayOf(name: String? = null, vararg entries: Any, init: ArrayHelper.() -> Unit = {}) {
        val b = ArrayHelper(name, entries)
        b.init()
        fields += b.build()
    }

    fun documentOf(name: String? = null, init: DocumentHelper.() -> Unit = {}) {
        val b = DocumentHelper(name)
        b.init()
        fields += b.build()
    }

    fun value(value: Any?) {
        val b = ValueHelper(null, value)
        fields += b.build()
    }
}

class DocumentHelper(val name: String? = null): FieldCollectionHelper<DocumentTemplate>() {
    override fun build(): DocumentTemplate {
        return DocumentTemplate(name, fields)
    }
}

private fun mapDocument(fields: MutableList<Field>): Map<String, Any?> {
    val map = mutableMapOf<String, Any?>()

    fields.forEach { field ->
        when (field) {
            is PrimitiveField -> {
                map[field.name] = field.value
            }

            is DocumentArray -> {
                map[field.name!!] = mapArray(field.fields)
            }

            is DocumentTemplate -> {
                map[field.name!!] = Document(mapDocument(field.fields))
            }
        }
    }

    return map
}

private fun mapArray(fields: MutableList<Field>): MutableList<Any> {
    return fields.map { field ->
        when (field) {
            is PrimitiveField -> field.value
            is Value -> field.value
            is DocumentArray -> mapArray(field.fields)
            is DocumentTemplate -> Document(mapDocument(field.fields))
            else -> null
        }
    }.filterNotNull().toMutableList()
}

class ArrayHelper(val name: String?, entries: Array<out Any>): FieldCollectionHelper<DocumentArray>() {

    init {
        entries.forEach {
            fields += Value(it)
        }
    }

    override fun build(): DocumentArray {
        return DocumentArray(name, fields)
    }
}

class ValueHelper(val name: String? = null, val value: Any?) : FieldCollectionHelper<Any>() {
    override fun build(): Value {
        return Value(value)
    }
}

class PrimitiveField(override val name: String, val value: Any?): Field

val registry = CodecRegistries.fromProviders(
    DocumentCodecProvider(),
    BsonValueCodecProvider(),
    ValueCodecProvider()
)