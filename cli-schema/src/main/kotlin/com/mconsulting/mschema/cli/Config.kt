package com.mconsulting.mschema.cli

import com.mconsulting.mrelational.schema.extractor.Namespace
import com.mconsulting.mrelational.schema.extractor.OutputFormat
import com.mongodb.MongoClientURI
import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.InvalidArgumentException
import com.xenomachina.argparser.SystemExitException
import com.xenomachina.argparser.default
import java.io.File

class Config(parser: ArgParser) {
    val logging = LoggingConfig(parser)
    val general = GeneralConfig(parser)
    val schema = SchemaConfig(parser)

    fun validate() {
        general.validate()
        schema.validate()
    }
}

class LoggingConfig(parser: ArgParser) {
    val quiet by parser.flagging("--quiet", help = "Logging: turn off all logging").default(false)
    val verbosity by parser.counting("-v", "--verbosity", help = "Logging: verbosity of logging (repeatable)")
        .default(0)
    val logPath by parser.storing("--logpath", help = "Logging: log file to send write to instead of stdout - has to be a file, not directory")
        .default<String?>(null)
}

/**
 * Extract schemas example options
 *   --extract
 *   --uri mongodb://localhost:27017
 *   --namespace db1.users:1000     <db.collection:sampleSize (int = 0 means all)>
 *   --namespace db1.groups:1000
 *   --format (schema/json-schema-v4)
 *   --output-directory ./
 */
class GeneralConfig(parser: ArgParser) {
    fun validate() {
    }

    val version by parser.option<Unit>("--version", help = "General: display the version") {
        throw ShowVersionException("version      : ${App.version}${System.lineSeparator()}git revision : ${App.gitRev}")
    }.default(false)

    val extract by parser.flagging("--extract", help = "General: Extract schemas from MongoDB").default(true)
}

class SchemaConfig(parser: ArgParser) {
    fun validate() {
        if (namespaces.isEmpty()) {
            throw IllegalArgumentException("at least one --namespace must be specified")
        }
    }

    val outputFormat by parser.storing("--format", help = """Schema: Output format for schema extractor, one of ["schema", "json-schema-v4"], ex: [--output schema]""") {
        when (this.toLowerCase()) {
            "schema" -> OutputFormat.SCHEMA
            "json-schema-v4" -> OutputFormat.JSON_SCHEMA_V4
            else -> throw InvalidArgumentException("""Output format for schema extractor, one of ["schema", "json-schema-v4"], ex: [--output schema]""")
        }
    }.default(OutputFormat.SCHEMA)

    val outputDirectory by parser.storing("--output-directory", help = """Schema: Output directory for the extracted schemas, ex: [--output-directory ./]""") {
        val file = File(this)
        if (!file.isDirectory) {
            throw IllegalArgumentException("$this is not a valid directory path, directory not found")
        }

        file
    }.default(File("./"))

    val uri by parser.storing("--uri", help = "Schema: MongoDB URI connection string [--uri mongodb://localhost:27017]") {
        MongoClientURI(this)
    }

    val namespaces by parser.adding("--namespace", help = "Schema: Add a namespace to extract the schema from, format <db.collection:sampleSize (int = 0 means all)>, ex: [db1.coll:1000]") {
        val parts = this.split(":")

        if (parts.size != 2) {
            throw IllegalArgumentException("--namespace must be of format <db.collection:sampleSize (int = 0 means all)>, ex: [db1.coll:1000]")
        }

        // Second part must be an integer
        try {
            parts.last().toLong()
        } catch (ex: NumberFormatException) {
            throw IllegalArgumentException("--namespace must be of format <db.collection:sampleSize (int = 0 means all)>, ex: [db1.coll:1000]")
        }

        val namespaceParts = parts.first().split(".")

        // Validate the namespace
        if (namespaceParts.size != 2) {
            throw IllegalArgumentException("--namespace must be of format <db.collection:sampleSize (int = 0 means all)>, ex: [db1.coll:1000]")
        }

        Namespace(namespaceParts.first(), namespaceParts.last(), parts.last().toLong())
    }
}

class ShowVersionException(version: String) : SystemExitException(version, 0)
