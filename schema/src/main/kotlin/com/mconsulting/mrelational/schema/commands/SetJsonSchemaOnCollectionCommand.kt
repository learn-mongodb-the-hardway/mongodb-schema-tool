package com.mconsulting.mrelational.schema.commands

import com.mongodb.MongoClient
import com.mongodb.client.model.ValidationLevel
import org.bson.BsonDocument
import org.bson.BsonString

data class ValidationOptions(val validationLevel: ValidationLevel)

class SetJsonSchemaOnCollectionCommand(val client: MongoClient) {
    fun execute(db: String, collection: String, jsonSchema: BsonDocument, options: ValidationOptions) {
        val database = client.getDatabase(db)
        val result = database.runCommand(
            BsonDocument()
                .append("collMod", BsonString(collection))
                .append("validator", BsonDocument()
                    .append("\$jsonSchema", jsonSchema))
                .append("validationLevel", BsonString(options.validationLevel.name.toLowerCase()))
        )
    }
}