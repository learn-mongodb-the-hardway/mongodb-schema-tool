package com.mconsulting.mrelational.schema.json

import com.mconsulting.mrelational.schema.Schema
import org.bson.BsonDocument

interface JsonGenerator {
    fun generate(schema: Schema): BsonDocument
}