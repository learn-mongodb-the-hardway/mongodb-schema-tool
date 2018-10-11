package com.mconsulting.mschema.cli

import com.mconsulting.mrelational.schema.extractor.Namespace
import com.xenomachina.argparser.ArgParser
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals

class ConfigTest {

    @Test
    fun shouldThrowDueToIllegalMongoDBURI() {
        val exception = assertThrows<IllegalArgumentException> {
            ArgParser(listOf("--uri", "mong://localhost:27017").toTypedArray()).parseInto(::Config)
        }

        assert(exception.message!!.contains("The connection string is invalid"))
    }

    @Test
    fun shouldCorrectlyParseNamespace() {
        val config = ArgParser(listOf(
            "--uri", "mongodb://localhost:27017",
            "--namespace", "db1.coll:1000"
        ).toTypedArray()).parseInto(::Config)

        assertEquals(1, config.schema.namespaces.size)
        assertEquals(Namespace("db1", "coll", 1000), config.schema.namespaces.first())
    }

    @Test
    fun shouldFailToParseNamespace() {
        val exception = assertThrows<IllegalArgumentException> {
            ArgParser(listOf(
                "--uri", "mongodb://localhost:27017",
                "--namespace", "db1.coll:test"
            ).toTypedArray()).parseInto(::Config)
        }

        assert(exception.message!!.contains("--namespace must be of format <db.collection:sampleSize (int = 0 means all)>, ex: [db1.coll:1000]"))
    }

    @Test
    fun shouldFailToLocateDirectory() {
        val exception = assertThrows<IllegalArgumentException> {
            ArgParser(listOf(
                "--uri", "mongodb://localhost:27017",
                "--output-directory", ":fdsafdsfsd"
            ).toTypedArray()).parseInto(::Config)
        }

        assert(exception.message!!.contains(":fdsafdsfsd is not a valid directory path, directory not found"))
    }

//    @Test
//    fun shouldTriggerHelp() {
////        val exception = assertThrows<IllegalArgumentException> {
//            ArgParser(listOf(
//                "--help"
//            ).toTypedArray()).parseInto(::Config)
////        }
////
////        assert(exception.message!!.contains(":fdsafdsfsd is not a valid directory path, directory not found"))
//    }
}