package com.mconsulting.mrelational.schema.json

import com.mconsulting.mrelational.schema.Schema
import org.bson.BsonDocument

class SchemaGeneratorOptions

class SchemaGenerator(val options: SchemaGeneratorOptions) : JsonGenerator {
    override fun generate(schema: Schema): BsonDocument {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}